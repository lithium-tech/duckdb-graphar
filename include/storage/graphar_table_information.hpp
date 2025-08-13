#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "storage/graphar_catalog.hpp"
#include "storage/graphar_table_entry.hpp"

namespace duckdb {
class GraphArTableSchema;
struct CreateTableInfo;
class GraphArSchemaEntry;

struct GraphArTableInformation {
   public:
    GraphArTableInformation(GraphArCatalog& catalog, unique_ptr<GraphArTableEntry> entry, const string& name)
        : catalog(catalog), entry(std::move(entry)), name(name) {}

   public:
    optional_ptr<CatalogEntry> GetSchemaVersion(optional_ptr<BoundAtClause> at);
    optional_ptr<CatalogEntry> CreateSchemaVersion(GraphArTableSchema& table_schema);
    const string& BaseFilePath() const;

    bool IsFilled() const { return filled; }
    GraphArTableEntry& GetEntry() const { return *entry; }

   private:
    GraphArCatalog& catalog;
    unique_ptr<GraphArTableEntry> entry;
    string name;
    bool filled = false;
};
}  // namespace duckdb
