#include "storage/graphar_table_entry.hpp"

#include "functions/table/read_edges.hpp"
#include "functions/table/read_vertices.hpp"
#include "storage/graphar_table_information.hpp"
#include "utils/func.hpp"
#include "utils/global_log_manager.hpp"

#include <duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp>
#include <duckdb/common/multi_file/multi_file_reader.hpp>
#include <duckdb/main/database.hpp>
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
    throw NotImplementedException("GraphArTableEntry::GetStatistics");
}

TableFunction GraphArTableEntry::GetScanFunction(ClientContext& context, unique_ptr<FunctionData>& bind_data) {
    throw NotImplementedException("GraphArTableEntry::GetScanFunction");
}

TableFunction GraphArTableEntry::GetScanFunction(ClientContext& context, unique_ptr<FunctionData>& bind_data,
                                                 const EntryLookupInfo& lookup) {
    DUCKDB_GRAPHAR_LOG_TRACE("GraphArTableEntry::GetScanFunction");
    auto bind_data_ = make_uniq<ReadBindData>();
    auto tmp_table_info = table_info.lock();
    const auto graph_info = tmp_table_info->GetCatalog().GetGraphInfo();
    const auto& type_info = tmp_table_info->GetTypeInfo();
    if (std::holds_alternative<std::shared_ptr<graphar::VertexInfo>>(type_info)) {
        const auto& vertex_info = std::get<std::shared_ptr<graphar::VertexInfo>>(type_info);
        ReadVertices::SetBindData(graph_info, vertex_info, bind_data_);
        bind_data = std::move(bind_data_);
        return ReadVertices::GetScanFunction();
    } else {
        const auto& edge_info = std::get<std::shared_ptr<graphar::EdgeInfo>>(type_info);
        ReadEdges::SetBindData(graph_info, edge_info, bind_data_);
        bind_data = std::move(bind_data_);
        return ReadEdges::GetScanFunction();
    }
}

TableStorageInfo GraphArTableEntry::GetStorageInfo(ClientContext& context) {
    TableStorageInfo result;
    if (table_info.expired()) {
        throw InvalidInputException("GraphArTableEntry::GetStorageInfo: table_info is expired");
    }
    auto tmp_table_info = table_info.lock();
    result.cardinality = GetCountClass::GetCount(tmp_table_info->GetTypeInfo(),
                                                 tmp_table_info->GetCatalog().GetGraphInfo()->GetPrefix());
    return result;
}

void GraphArTableEntry::BindUpdateConstraints(Binder& binder, LogicalGet&, LogicalProjection&, LogicalUpdate&,
                                              ClientContext&) {
    throw NotImplementedException("GraphArTableEntry::BindUpdateConstraints");
}

}  // namespace duckdb
