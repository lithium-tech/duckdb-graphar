
#pragma once

#include "storage/graphar_catalog_set.hpp"
#include "storage/graphar_table_entry.hpp"
#include "storage/graphar_table_information.hpp"

#include <duckdb/catalog/catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>

namespace duckdb {

class GraphArTableSet : public GraphArCatalogSet {
public:
    explicit GraphArTableSet(GraphArSchemaEntry& schema) : GraphArCatalogSet(schema) {}

public:
    static unique_ptr<GraphArTableInformation> GetTableInfo(ClientContext& context, GraphArSchemaEntry& schema,
                                                            const string& table_name);
    optional_ptr<CatalogEntry> GetEntry(ClientContext& context, const EntryLookupInfo& lookup) override;
    void Scan(ClientContext& context, const std::function<void(CatalogEntry&)>& callback) override;
    optional_ptr<CatalogEntry> CreateNewEntry(ClientContext& context, Catalog& catalog, GraphArSchemaEntry& schema,
                                              CreateTableInfo& info);
    optional_ptr<CatalogEntry> CreateNewEntry(ClientContext& context, Catalog& catalog, GraphArSchemaEntry& schema,
                                              CreateViewInfo& info);
    void LoadEntries(ClientContext& context);
    void FillEntry(ClientContext& context, GraphArTableInformation& table);

    template <typename InfoVector>
    std::enable_if_t<std::is_same_v<InfoVector, graphar::VertexInfoVector> ||
                         std::is_same_v<InfoVector, graphar::EdgeInfoVector>,
                     void>
    CreateTables(GraphArCatalog& graphar_catalog, const InfoVector& infos, GraphArTableType type);

private:
    case_insensitive_map_t<shared_ptr<GraphArTableInformation>> table_entries;
    case_insensitive_map_t<shared_ptr<ViewCatalogEntry>> view_entries;
};

}  // namespace duckdb
