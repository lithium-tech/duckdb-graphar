#pragma once

#include "storage/graphar_catalog.hpp"
#include "storage/graphar_table_entry.hpp"
#include "utils/type_info.hpp"

#include <duckdb/catalog/catalog_entry.hpp>

namespace duckdb {
class GraphArTableSchema;

enum class GraphArTableType { Vertex, Edge, Unknown };

struct GraphArTableInformation {
public:
    GraphArTableInformation(GraphArCatalog& catalog_, unique_ptr<GraphArTableEntry> entry_, const string& name_,
                            GraphArTableType type_, TypeInfoPtr type_info_)
        : catalog(catalog_), entry(std::move(entry_)), name(name_), type(type_), type_info(type_info_) {}

public:
    optional_ptr<CatalogEntry> GetSchemaVersion(optional_ptr<BoundAtClause> at);
    optional_ptr<CatalogEntry> CreateSchemaVersion(GraphArTableSchema& table_schema);
    const string& BaseFilePath() const;

    bool IsFilled() const { return filled; }
    GraphArTableEntry& GetEntry() const { return *entry; }
    GraphArTableType GetType() const { return type; }

    const GraphArCatalog& GetCatalog() const { return catalog; }
    const TypeInfoPtr& GetTypeInfo() const { return type_info; }

private:
    GraphArCatalog& catalog;
    unique_ptr<GraphArTableEntry> entry;
    string name;
    bool filled = false;
    GraphArTableType type = GraphArTableType::Unknown;
    TypeInfoPtr type_info;
};
}  // namespace duckdb
