#include "storage/graphar_table_set.hpp"

#include "functions/table/read_base.hpp"
#include "functions/table/read_edges.hpp"
#include "functions/table/read_vertices.hpp"
#include "storage/graphar_schema_entry.hpp"
#include "storage/graphar_table_information.hpp"
#include "storage/graphar_transaction.hpp"
#include "utils/func.hpp"

#include <duckdb/catalog/dependency_list.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/parser/constraints/list.hpp>
#include <duckdb/parser/constraints/not_null_constraint.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/parsed_data/create_schema_info.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/planner/parsed_data/bound_create_table_info.hpp>

#include <filesystem>

namespace duckdb {

GraphArTableSet::GraphArTableSet(GraphArSchemaEntry& schema) : schema(schema), catalog(schema.ParentCatalog()) {}

void GraphArTableSet::FillEntry(ClientContext& context, GraphArTableInformation& table) {
    DUCKDB_GRAPHAR_LOG_TRACE("GraphArTableSet::FillEntry");
    if (table.IsFilled()) {
        return;
    }

    auto& graphar_catalog = catalog.Cast<GraphArCatalog>();
}

void GraphArTableSet::Scan(ClientContext& context, const std::function<void(CatalogEntry&)>& callback) {
    DUCKDB_GRAPHAR_LOG_TRACE("GraphArTableSet::Scan");
    lock_guard<mutex> l(entry_lock);
    LoadEntries(context);
    for (auto& entry : entries) {
        auto& table_info = entry.second;
        FillEntry(context, *table_info);
        callback(table_info->GetEntry());
    }
}

template <typename InfoVector>
std::enable_if_t<
    std::is_same_v<InfoVector, graphar::VertexInfoVector> || std::is_same_v<InfoVector, graphar::EdgeInfoVector>, void>
GraphArTableSet::CreateTables(GraphArCatalog& graphar_catalog, const InfoVector& infos, GraphArTableType type) {
    DUCKDB_GRAPHAR_LOG_TRACE("GraphArTableSet::CreateTables");

    for (auto& info : infos) {
        auto type_info = make_uniq<CreateTableInfo>();
        auto file_name = GraphArFunctions::GetNameFromInfo(info);
        type_info->table = file_name;

        auto bind_data = make_uniq<ReadBindData>();
        auto& graphar_catalog = catalog.Cast<GraphArCatalog>();
        if constexpr (std::is_same_v<InfoVector, graphar::VertexInfoVector>) {
            ReadVertices::SetBindData(graphar_catalog.GetGraphInfo(), *info, bind_data);
        } else {
            ReadEdges::SetBindData(graphar_catalog.GetGraphInfo(), *info, bind_data);
        }

        vector<ColumnDefinition> columns;
        for (idx_t i = 0; i < bind_data->GetFlattenPropNames().size(); ++i) {
            columns.emplace_back(bind_data->GetFlattenPropNames()[i],
                                 GraphArFunctions::graphArT2duckT(bind_data->GetFlattenPropTypes()[i]));
        }
        type_info->columns = ColumnList(std::move(columns));

        CreateSchemaInfo fake_schema_info;
        auto schema = make_uniq<GraphArSchemaEntry>(catalog, fake_schema_info);
        auto table_entry = make_uniq<GraphArTableEntry>(catalog, std::move(schema), *type_info);
        auto table_info = make_shared_ptr<GraphArTableInformation>(graphar_catalog, std::move(table_entry), file_name,
                                                                   type, bind_data->GetParams());
        table_info->GetEntry().SetTableInfo(table_info);
        entries[file_name] = std::move(table_info);
        DUCKDB_GRAPHAR_LOG_INFO("Table was created with name " + file_name);
    }
}

void GraphArTableSet::LoadEntries(ClientContext& context) {
    DUCKDB_GRAPHAR_LOG_TRACE("GraphArTableSet::LoadEntries");
    if (!entries.empty()) {
        return;
    }

    auto& graphar_catalog = catalog.Cast<GraphArCatalog>();

    CreateTables(graphar_catalog, graphar_catalog.GetGraphInfo()->GetVertexInfos(), GraphArTableType::Vertex);
    CreateTables(graphar_catalog, graphar_catalog.GetGraphInfo()->GetEdgeInfos(), GraphArTableType::Edge);

    DUCKDB_GRAPHAR_LOG_TRACE("Exiting GraphArTableSet::LoadEntries");
}

unique_ptr<GraphArTableInformation> GraphArTableSet::GetTableInfo(ClientContext& context, GraphArSchemaEntry& schema,
                                                                  const string& table_name) {
    throw NotImplementedException("GraphArTableSet::GetTableInformation");
}

optional_ptr<CatalogEntry> GraphArTableSet::GetEntry(ClientContext& context, const EntryLookupInfo& lookup) {
    DUCKDB_GRAPHAR_LOG_TRACE("GraphArTableSet::GetEntry");
    LoadEntries(context);
    lock_guard<mutex> l(entry_lock);
    auto entry = entries.find(lookup.GetEntryName());
    if (entry == entries.end()) {
        return nullptr;
    }
    FillEntry(context, *entry->second);
    return entry->second->GetEntry();
}

}  // namespace duckdb
