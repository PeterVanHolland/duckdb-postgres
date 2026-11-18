#include "storage/postgres_catalog_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"

#include "postgres_result.hpp"
#include "storage/postgres_catalog.hpp"
#include "storage/postgres_transaction.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "storage/postgres_schema_entry.hpp"

namespace duckdb {

PostgresCatalogSet::PostgresCatalogSet(Catalog &catalog, bool is_loaded_p)
    : catalog(catalog), is_loaded(is_loaded_p), loading_thread(thread_id()) {
}

PostgresCatalogSet::catalog_entry_map_t::iterator PostgresCatalogSet::FindEntryLocked(const string &name) {
	auto entry = entries.find(name);
	if (entry != entries.end()) {
		return entry;
	}
	// the case insensitive map holds the stored spelling of the name
	auto name_entry = entry_map.find(name);
	if (name_entry == entry_map.end()) {
		return entries.end();
	}
	return entries.find(name_entry->second);
}

optional_ptr<CatalogEntry> PostgresCatalogSet::GetEntry(ClientContext &context, PostgresTransaction &transaction,
                                                        const string &name) {
	if (SupportReload() && catalog.Cast<PostgresCatalog>().lazy_load) {
		lock_guard<mutex> lock(load_lock);
		if (!is_loaded) {
			return LookupEntry(context, transaction, name);
		}
	}
	{
		auto l = LoadEntriesForRead(context, transaction);
		auto entry = FindEntryLocked(name);
		if (entry != entries.end()) {
			return transaction.ReferenceEntry(entry->second);
		}
	}
	// entry not found
	if (SupportReload()) {
		lock_guard<mutex> lock(load_lock);
		// try loading entries again - maybe there has been a change remotely
		auto entry = ReloadEntry(transaction, name);
		if (entry) {
			return entry;
		}
	}
	return nullptr;
}

static string ComputeStalenessSignature(PostgresResult &result) {
	if (result.ColumnCount() < 3) {
		throw InvalidInputException(
		    "pg_staleness_query must return at least 3 columns (identity, name, revision marker), got %llu",
		    result.ColumnCount());
	}
	string signature;
	auto rows = result.Count();
	for (idx_t row = 0; row < rows; row++) {
		signature += result.GetString(row, 0);
		signature += '\0';
		signature += result.GetString(row, 1);
		signature += '\0';
		signature += result.GetString(row, 2);
		signature += '\x01';
	}
	return signature;
}

//! Runs the staleness query on a short-lived pooled connection, not the caller's own.
//! Avoids pinning the caller's connection (and, under PgBouncer transaction-mode pooling,
//! a backend slot) for the round-trip.
static unique_ptr<PostgresResult> RunStalenessQuery(Catalog &catalog, ClientContext &context,
                                                    const string &staleness_query) {
	auto connection = catalog.Cast<PostgresCatalog>().GetConnectionPool().Acquire();
	return connection.GetConnection().Query(context, staleness_query);
}

void PostgresCatalogSet::RefreshStalenessSignature(PostgresTransaction &transaction, bool use_transaction_connection) {
	auto &context = *transaction.GetContext();
	auto staleness_query = GetStalenessQuery(context);
	if (staleness_query.empty()) {
		return;
	}
	auto result = use_transaction_connection ? transaction.Query(staleness_query)
	                                         : RunStalenessQuery(catalog, context, staleness_query);
	auto signature = ComputeStalenessSignature(*result);
	if (transaction.GetTransactionState() == PostgresTransactionState::TRANSACTION_STARTED) {
		// transaction still open - only valid on commit, since xmin reverts on rollback
		transaction.StageStalenessSignature(*this, std::move(signature));
		return;
	}
	staleness_signature = std::move(signature);
}

void PostgresCatalogSet::PromoteStalenessSignature(string signature) {
	// called from Commit(), possibly a different thread than the one that staged it
	lock_guard<mutex> lock(load_lock);
	staleness_signature = std::move(signature);
}

void PostgresCatalogSet::LoadEntriesLocked(ClientContext &context, PostgresTransaction &transaction) {
	if (catalog.Cast<PostgresCatalog>().lazy_load) {
		// the full load replaces the entries looked up one by one
		ClearEntriesLocked();
	}
	loading_thread = ThreadUtil::GetThreadId();
	try {
		LoadEntries(context, transaction);
	} catch (...) {
		loading_thread = thread_id();
		throw;
	}
	RefreshStalenessSignature(transaction, /*use_transaction_connection=*/false);
	is_loaded = true;
	loading_thread = thread_id();
}

unique_lock<mutex> PostgresCatalogSet::LoadEntriesForRead(ClientContext &context, PostgresTransaction &transaction) {
	while (true) {
		TryLoadEntries(context, transaction);
		unique_lock<mutex> l(entry_lock);
		// the loading thread may read partially loaded entries
		if (is_loaded || loading_thread == ThreadUtil::GetThreadId()) {
			return l;
		}
	}
}

void PostgresCatalogSet::TryLoadEntries(ClientContext &context, PostgresTransaction &transaction) {
	if (HasInternalDependencies()) {
		if (is_loaded || loading_thread == ThreadUtil::GetThreadId()) {
			return;
		}
	}
	if (is_loaded) {
		auto staleness_query = GetStalenessQuery(context);
		if (staleness_query.empty()) {
			return;
		}
		// runs outside load_lock - avoids blocking other threads on this thread's network round-trip
		auto result = RunStalenessQuery(catalog, context, staleness_query);
		auto signature = ComputeStalenessSignature(*result);

		lock_guard<mutex> lock(load_lock);
		if (is_loaded) {
			if (signature == staleness_signature) {
				return;
			}
			ClearEntriesLocked();
		}
		// else: someone else cleared/reloaded while we were checking - fall through to the
		// reload below, still holding load_lock.
		LoadEntriesLocked(context, transaction);
		return;
	}

	lock_guard<mutex> lock(load_lock);
	if (is_loaded) {
		// someone else already loaded while we were waiting for the lock.
		return;
	}
	LoadEntriesLocked(context, transaction);
}

bool PostgresCatalogSet::EntriesStale(ClientContext &context) {
	auto staleness_query = GetStalenessQuery(context);
	if (staleness_query.empty()) {
		return false;
	}
	auto result = RunStalenessQuery(catalog, context, staleness_query);
	if (ComputeStalenessSignature(*result) == staleness_signature) {
		return false;
	}
	ClearEntriesLocked();
	return true;
}

optional_ptr<CatalogEntry> PostgresCatalogSet::LookupEntry(ClientContext &context, PostgresTransaction &transaction,
                                                           const string &name) {
	shared_ptr<CatalogEntry> cached;
	bool has_entries;
	{
		lock_guard<mutex> l(entry_lock);
		has_entries = !entries.empty();
		auto entry = entries.find(name);
		if (entry == entries.end() && missing_spellings.count(name)) {
			entry = FindEntryLocked(name);
		}
		if (entry != entries.end()) {
			cached = entry->second;
		}
	}
	// a change on the server drops the cached entries, whether the name is among them or not
	if (has_entries && EntriesStale(context)) {
		cached.reset();
	}
	if (cached) {
		return transaction.ReferenceEntry(cached);
	}
	LoadEntriesByName(transaction, name);
	RefreshStalenessSignature(transaction, /*use_transaction_connection=*/false);
	lock_guard<mutex> l(entry_lock);
	auto entry = FindEntryLocked(name);
	if (entry == entries.end()) {
		return nullptr;
	}
	if (entry->first != name) {
		// the exact spelling has no entry on the server, so its case insensitive match serves it from now on
		missing_spellings.insert(name);
	}
	return transaction.ReferenceEntry(entry->second);
}

void PostgresCatalogSet::LoadEntriesByName(PostgresTransaction &transaction, const string &name) {
	throw InternalException("PostgresCatalogSet does not support LoadEntriesByName");
}

optional_ptr<CatalogEntry> PostgresCatalogSet::ReloadEntry(PostgresTransaction &transaction, const string &name) {
	throw InternalException("PostgresCatalogSet does not support ReloadEntry");
}

void PostgresCatalogSet::DropEntry(PostgresTransaction &transaction, DropInfo &info) {
	string drop_query = "DROP ";
	drop_query += CatalogTypeToString(info.type) + " ";
	if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
		drop_query += " IF EXISTS ";
	}
	if (!info.GetQualifiedName().Schema().empty() && info.type != CatalogType::SCHEMA_ENTRY) {
		drop_query += PostgresUtils::WriteIdentifier(info.GetQualifiedName().Schema().GetIdentifierName()) + ".";
	}
	drop_query += PostgresUtils::WriteIdentifier(info.GetQualifiedName().Name().GetIdentifierName());
	if (info.cascade) {
		drop_query += " CASCADE";
	}
	transaction.Query(drop_query);

	// erase the entry from the catalog set
	{
		lock_guard<mutex> l(entry_lock);
		entries.erase(info.GetQualifiedName().Name().GetIdentifierName());
	}
	RefreshStalenessSignature(transaction, /*use_transaction_connection=*/true);
}

void PostgresCatalogSet::Scan(ClientContext &context, PostgresTransaction &transaction,
                              const std::function<void(CatalogEntry &)> &callback) {
	auto l = LoadEntriesForRead(context, transaction);
	for (auto &entry : entries) {
		transaction.ReferenceEntry(entry.second);
		callback(*entry.second);
	}
}

optional_ptr<CatalogEntry> PostgresCatalogSet::CreateEntry(PostgresTransaction &transaction,
                                                           shared_ptr<CatalogEntry> entry) {
	lock_guard<mutex> l(entry_lock);
	auto result = transaction.ReferenceEntry(entry);
	if (result->name.empty()) {
		throw InternalException("PostgresCatalogSet::CreateEntry called with empty name");
	}
	entry_map.insert(make_pair(result->name, result->name));
	entries.insert(make_pair(result->name, std::move(entry)));
	return result;
}

void PostgresCatalogSet::InvalidateEntry(const string &name) {
	lock_guard<mutex> l(entry_lock);
	auto entry = FindEntryLocked(name);
	if (entry != entries.end()) {
		entries.erase(entry);
	}
}

void PostgresCatalogSet::ClearEntries() {
	lock_guard<mutex> load_guard(load_lock);
	ClearEntriesLocked();
}

void PostgresCatalogSet::ClearEntriesLocked() {
	lock_guard<mutex> entry_guard(entry_lock);
	entry_map.clear();
	entries.clear();
	missing_spellings.clear();
	is_loaded = false;
}

PostgresInSchemaSet::PostgresInSchemaSet(PostgresSchemaEntry &schema, bool is_loaded)
    : PostgresCatalogSet(schema.ParentCatalog(), is_loaded), schema(schema) {
}

PostgresResultSlice::PostgresResultSlice(unique_ptr<PostgresResult> result_p)
    : result(result_p.release()), start(0), end(result->Count()) {
}

optional_ptr<CatalogEntry> PostgresInSchemaSet::CreateEntry(PostgresTransaction &transaction,
                                                            shared_ptr<CatalogEntry> entry) {
	entry->internal = schema.internal;
	return PostgresCatalogSet::CreateEntry(transaction, std::move(entry));
}

} // namespace duckdb
