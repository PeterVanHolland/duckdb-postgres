//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_catalog_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {
struct DropInfo;
class PostgresResult;
class PostgresSchemaEntry;
class PostgresTransaction;

class PostgresCatalogSet {
public:
	PostgresCatalogSet(Catalog &catalog, bool is_loaded);

	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, PostgresTransaction &transaction, const string &name);
	void DropEntry(PostgresTransaction &transaction, DropInfo &info);
	void Scan(ClientContext &context, PostgresTransaction &transaction,
	          const std::function<void(CatalogEntry &)> &callback);
	virtual optional_ptr<CatalogEntry> CreateEntry(PostgresTransaction &transaction, shared_ptr<CatalogEntry> entry);
	void ClearEntries();
	//! Remove an entry from the cache (without dropping it) - it is reloaded when it is looked up the next time
	void InvalidateEntry(const string &name);
	virtual bool SupportReload() const {
		return false;
	}
	virtual optional_ptr<CatalogEntry> ReloadEntry(PostgresTransaction &transaction, const string &name);

protected:
	virtual void LoadEntries(ClientContext &context, PostgresTransaction &transaction) = 0;
	//! Loads the entries whose name matches case insensitively, without loading the whole set
	virtual void LoadEntriesByName(PostgresTransaction &transaction, const string &name);
	//! Whether or not the catalog set contains dependencies to itself that have
	//! to be resolved WHILE loading
	virtual bool HasInternalDependencies() const {
		return false;
	}
	//! Empty (default) means staleness is never checked once loaded
	virtual string GetStalenessQuery(ClientContext &context) const {
		return string();
	}
	void TryLoadEntries(ClientContext &context, PostgresTransaction &transaction);
	void RefreshStalenessSignature(PostgresTransaction &transaction, bool use_transaction_connection);

private:
	using catalog_entry_map_t = unordered_map<string, shared_ptr<CatalogEntry>>;

	//! Runs LoadEntries + RefreshStalenessSignature, must be called with load_lock held
	void LoadEntriesLocked(ClientContext &context, PostgresTransaction &transaction);
	//! Must be called with load_lock held
	void ClearEntriesLocked();
	//! Returns with entry_lock held
	unique_lock<mutex> LoadEntriesForRead(ClientContext &context, PostgresTransaction &transaction);
	//! Finds an entry by its name or its case insensitive match, must be called with entry_lock held
	catalog_entry_map_t::iterator FindEntryLocked(const string &name);
	//! Looks one entry up without loading the whole set, false when the set turns out to be loaded
	bool LookupEntry(ClientContext &context, PostgresTransaction &transaction, const string &name,
	                 optional_ptr<CatalogEntry> &entry);
	//! Takes a signature, staged until the commit while the transaction is open
	void SetStalenessSignature(PostgresTransaction &transaction, string signature);

protected:
public:
	void PromoteStalenessSignature(string signature);

protected:
	Catalog &catalog;

private:
	mutex entry_lock;
	mutex load_lock;
	catalog_entry_map_t entries;
	case_insensitive_map_t<string> entry_map;
	//! Looked up spellings that have no entry of their own, which their case insensitive match serves
	unordered_set<string> missing_spellings;
	atomic<bool> is_loaded;
	atomic<thread_id> loading_thread;
	string staleness_signature;
};

class PostgresInSchemaSet : public PostgresCatalogSet {
public:
	PostgresInSchemaSet(PostgresSchemaEntry &schema, bool is_loaded);

	optional_ptr<CatalogEntry> CreateEntry(PostgresTransaction &transaction, shared_ptr<CatalogEntry> entry) override;

protected:
	PostgresSchemaEntry &schema;
};

struct PostgresResultSlice {
	PostgresResultSlice(shared_ptr<PostgresResult> result_p, idx_t start, idx_t end)
	    : result(std::move(result_p)), start(start), end(end) {
	}
	//! The whole result
	explicit PostgresResultSlice(unique_ptr<PostgresResult> result);

	PostgresResult &GetResult() {
		return *result;
	}

	shared_ptr<PostgresResult> result;
	idx_t start;
	idx_t end;
};

} // namespace duckdb
