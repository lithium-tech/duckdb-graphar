#include "storage/graphar_table_entry.hpp"

#include "functions/table/read_edges.hpp"
#include "functions/table/read_vertices.hpp"
#include "storage/graphar_table_information.hpp"
#include "utils/global_log_manager.hpp"

#include <duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp>
#include <duckdb/common/multi_file/multi_file_reader.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/extension_util.hpp>
#include <duckdb/main/secret/secret_manager.hpp>
#include <duckdb/parser/tableref/table_function_ref.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/logical_operator.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/tableref/bound_at_clause.hpp>
#include <duckdb/storage/statistics/base_statistics.hpp>
#include <duckdb/storage/table_storage_info.hpp>

namespace duckdb {

GraphArTableEntry::GraphArTableEntry(Catalog& catalog, unique_ptr<SchemaCatalogEntry> schema, CreateTableInfo& info)
    : TableCatalogEntry(catalog, *schema, info), schema(std::move(schema)) {}

unique_ptr<BaseStatistics> GraphArTableEntry::GetStatistics(ClientContext& context, column_t column_id) {
    return nullptr;
}

TableFunction GraphArTableEntry::GetScanFunction(ClientContext& context, unique_ptr<FunctionData>& bind_data) {
    throw NotImplementedException("GraphArTableEntry::GetScanFunction");
}

TableFunction GraphArTableEntry::GetScanFunction(ClientContext& context, unique_ptr<FunctionData>& bind_data,
                                                 const EntryLookupInfo& lookup) {
    DUCKDB_GRAPHAR_LOG_TRACE("GraphArTableEntry::GetScanFunction");
    auto bind_data_ = make_uniq<ReadBindData>();
    auto tmp_table_info = table_info.lock();
    switch (tmp_table_info->GetType()) {
        case GraphArTableType::Vertex:
            ReadVertices::SetBindData(
                tmp_table_info->GetCatalog().GetGraphInfo(),
                *tmp_table_info->GetCatalog().GetGraphInfo()->GetVertexInfo(tmp_table_info->GetParams()[0]),
                bind_data_);
            bind_data = std::move(bind_data_);
            return ReadVertices::GetScanFunction();
        case GraphArTableType::Edge:
            ReadEdges::SetBindData(
                tmp_table_info->GetCatalog().GetGraphInfo(),
                *tmp_table_info->GetCatalog().GetGraphInfo()->GetEdgeInfo(
                    tmp_table_info->GetParams()[0], tmp_table_info->GetParams()[1], tmp_table_info->GetParams()[2]),
                bind_data_);
            bind_data = std::move(bind_data_);
            return ReadEdges::GetScanFunction();
        default:
            throw InternalException("Unknown table type");
    }
}

TableStorageInfo GraphArTableEntry::GetStorageInfo(ClientContext& context) {
    TableStorageInfo result;
    // TODO fill info
    return result;
}

void GraphArTableEntry::BindUpdateConstraints(Binder& binder, LogicalGet&, LogicalProjection&, LogicalUpdate&,
                                              ClientContext&) {
    throw NotImplementedException("GraphArTableEntry::BindUpdateConstraints");
}

}  // namespace duckdb
