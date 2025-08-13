#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "storage/graphar_catalog.hpp"
#include "storage/graphar_table_entry.hpp"

namespace duckdb {
class GraphArTableSchema;

enum class GraphArTableType { Vertex, Edge, Unknown };

struct GraphArTableInformation {
   public:
    GraphArTableInformation(GraphArCatalog& catalog_, unique_ptr<GraphArTableEntry> entry_, const string& name_,
                            GraphArTableType type_, vector<string>&& params_)
        : catalog(catalog_), entry(std::move(entry_)), name(name_), type(type_), params(std::move(params_)) {}

public:
    optional_ptr<CatalogEntry> GetSchemaVersion(optional_ptr<BoundAtClause> at);
    optional_ptr<CatalogEntry> CreateSchemaVersion(GraphArTableSchema& table_schema);
    const string& BaseFilePath() const;

    bool IsFilled() const { return filled; }
    GraphArTableEntry& GetEntry() const { return *entry; }
    GraphArTableType GetType() const { return type; }

    const GraphArCatalog& GetCatalog() const { return catalog; }
    const vector<string>& GetParams() const { return params; }

private:
    GraphArCatalog& catalog;
    unique_ptr<GraphArTableEntry> entry;
    string name;
    bool filled = false;
    GraphArTableType type = GraphArTableType::Unknown;
    vector<std::string> params;
};
}  // namespace duckdb
