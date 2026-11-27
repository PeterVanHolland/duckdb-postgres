//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_schema_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/postgres_catalog_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "storage/postgres_schema_entry.hpp"

namespace duckdb {
struct CreateSchemaInfo;

class PostgresSchemaSet : public PostgresCatalogSet {
public:
	PostgresSchemaSet(Catalog &catalog, vector<string> schemas_to_load, bool lazy_load);

public:
	optional_ptr<CatalogEntry> CreateSchema(PostgresTransaction &transaction, CreateSchemaInfo &info);

	static string GetInitializeQuery(const vector<string> &schemas);
	static string GetInitializeQueryInformationSchema(const vector<string> &schemas);

protected:
	void LoadEntries(ClientContext &context, PostgresTransaction &transaction) override;
	void LoadEntriesInformationSchema(ClientContext &context, PostgresTransaction &transaction);
	void LoadSchemasOnly(ClientContext &context, PostgresTransaction &transaction);

protected:
	//! Schema to load - if empty loads all schemas (default behavior)
	vector<string> schemas_to_load;
	//! Load the schemas without their tables, types and indexes, which load when they are first used
	bool lazy_load;
};

} // namespace duckdb
