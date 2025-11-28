#include "functions/table/read_edges.hpp"

#include "utils/benchmark.hpp"
#include "utils/func.hpp"

#include <arrow/c/bridge.h>

#include <duckdb/common/named_parameter_map.hpp>
#include <duckdb/function/table/arrow.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>

#include <graphar/api/arrow_reader.h>
#include <graphar/api/high_level_reader.h>
#include <graphar/arrow/chunk_reader.h>
#include <graphar/expression.h>
#include <graphar/fwd.h>

#include <iostream>

namespace duckdb {
//-------------------------------------------------------------------
// GetBindData
//-------------------------------------------------------------------
void ReadEdges::SetBindData(std::shared_ptr<graphar::GraphInfo> graph_info,
                            std::shared_ptr<graphar::EdgeInfo> edge_info, unique_ptr<ReadBindData>& bind_data) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::SetBindData");
    ReadBase::SetBindData(graph_info, edge_info, bind_data, "read_edges", 0, 1, {SRC_GID_COLUMN, DST_GID_COLUMN});
}
//-------------------------------------------------------------------
// Bind
//-------------------------------------------------------------------
unique_ptr<FunctionData> ReadEdges::Bind(ClientContext& context, TableFunctionBindInput& input,
                                         vector<LogicalType>& return_types, vector<string>& names) {
    bool time_logging = GraphArSettings::is_time_logging(context);

    ScopedTimer t("Bind");

    DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::Bind Parse parameters");

    const auto file_path = StringValue::Get(input.inputs[0]);
    const std::string src_type = StringValue::Get(input.named_parameters.at("src"));
    const std::string dst_type = StringValue::Get(input.named_parameters.at("dst"));
    const std::string e_type = StringValue::Get(input.named_parameters.at("type"));

    DUCKDB_GRAPHAR_LOG_DEBUG(src_type + "--" + e_type + "->" + dst_type + "\nLoad Graph Info and Edge Info");

    auto bind_data = make_uniq<ReadBindData>();
    DUCKDB_GRAPHAR_LOG_DEBUG("file path " + file_path);
    auto maybe_graph_info = graphar::GraphInfo::Load(file_path);
    if (maybe_graph_info.has_error()) {
        throw IOException("Failed to load graph info from path: %s", file_path);
    }
    auto graph_info = maybe_graph_info.value();

    auto edge_info = graph_info->GetEdgeInfo(src_type, e_type, dst_type);
    if (!edge_info) {
        throw BinderException("Edges of this type are not found");
    }

    DUCKDB_GRAPHAR_LOG_DEBUG("Fill bind data");

    SetBindData(graph_info, edge_info, bind_data);

    names = bind_data->flatten_prop_names;
    std::transform(bind_data->flatten_prop_types.begin(), bind_data->flatten_prop_types.end(),
                   std::back_inserter(return_types),
                   [](const auto& return_type) { return GraphArFunctions::graphArT2duckT(return_type); });

    DUCKDB_GRAPHAR_LOG_DEBUG("Bind finish");
    if (time_logging) {
        t.print();
    }

    return bind_data;
}
//-------------------------------------------------------------------
// GetReader
//-------------------------------------------------------------------
std::shared_ptr<Reader> ReadEdges::GetReader(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate,
                                             ReadBindData& bind_data, idx_t ind, const std::string& filter_column) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::GetReader");
    graphar::AdjListType adj_list_type;
    if (filter_column == "" or filter_column == SRC_GID_COLUMN) {
        adj_list_type = graphar::AdjListType::ordered_by_source;
    } else if (filter_column == DST_GID_COLUMN) {
        adj_list_type = graphar::AdjListType::ordered_by_dest;
    } else {
        throw NotImplementedException("Only src and dst filters are supported");
    }
    auto edge_info = *std::get_if<std::shared_ptr<graphar::EdgeInfo>>(&bind_data.type_info);
    if (!edge_info) {
        throw InternalException("Failed to get edge info");
    }
    const auto& prefix = bind_data.graph_info->GetPrefix();
    if (ind == 0) {
        DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::GetReader: making src and dst reader...");
        if (edge_info->GetAdjacentList(adj_list_type)->GetFileType() == graphar::FileType::PARQUET) {
            DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::GetReader: making duckdb reader...");
            return ConvertReader(
                graphar::DuckAdjListChunkReader::Make(context, gstate.file_reader, edge_info, adj_list_type, prefix));
        } else {
            DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::GetReader: making arrow reader...");
            return ConvertReader(graphar::DuckAdjListArrowChunkReader::Make(context, edge_info, adj_list_type, prefix));
        }
    }
    DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::GetReader: making property reader...");
    if (edge_info->GetAdjacentList(adj_list_type)->GetFileType() == graphar::FileType::PARQUET) {
        DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::GetReader: making duckdb reader...");
        return ConvertReader(graphar::DuckAdjListPropertyChunkReader::Make(
            context, gstate.file_reader, edge_info, bind_data.pgs[ind - 1], adj_list_type, prefix));
    } else {
        DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::GetReader: making arrow reader...");
        return ConvertReader(graphar::DuckAdjListPropertyArrowChunkReader::Make(
            context, edge_info, bind_data.pgs[ind - 1], adj_list_type, prefix));
    }
}
//-------------------------------------------------------------------
// GetStatistics
//-------------------------------------------------------------------
unique_ptr<BaseStatistics> ReadEdges::GetStatistics(ClientContext& context, const FunctionData* bind_data,
                                                    column_t column_index) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::GetStatistics");
    auto read_bind_data = bind_data->Cast<ReadBindData>();
    if (column_index < 0 || column_index >= read_bind_data.GetFlattenPropTypes().size()) {
        return nullptr;
    }
    auto duck_type = GraphArFunctions::graphArT2duckT(read_bind_data.GetFlattenPropTypes()[column_index]);
    auto column_name = read_bind_data.GetFlattenPropNames()[column_index];
    if (column_name != SRC_GID_COLUMN && column_name != DST_GID_COLUMN) {
        auto stats = BaseStatistics::CreateUnknown(duck_type);
        return stats.ToUnique();
    }
    auto v_type = GetVertexTypeName(read_bind_data.type_info, column_name);
    auto stats = NumericStats::CreateEmpty(LogicalType::BIGINT);
    NumericStats::SetMin(stats, Value::BIGINT(0));
    NumericStats::SetMax(stats,
                         Value::BIGINT(GraphArFunctions::GetVertexNum(read_bind_data.GetGraphInfo(), v_type) - 1));
    DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::GetStatistics: finished");
    return stats.ToUnique();
}
//-------------------------------------------------------------------
// PushdownComplexFilter
//-------------------------------------------------------------------
void ReadEdges::PushdownComplexFilter(ClientContext& context, LogicalGet& get, FunctionData* bind_data,
                                      vector<unique_ptr<Expression>>& filters) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::PushdownComplexFilter");
    if (!bind_data) {
        throw InternalException("Bind data is nullptr");
    }
    auto read_bind_data = dynamic_cast<ReadBindData*>(bind_data);
    for (auto& pg : read_bind_data->pgs) {
        if (pg->GetFileType() != graphar::FileType::PARQUET) {
            // our pushdown works greatly only for parquet files
            return;
        }
    }
    auto edge_info = *std::get_if<std::shared_ptr<graphar::EdgeInfo>>(&read_bind_data->type_info);
    vector<unique_ptr<Expression>> filters_new;
    bool already_pushed = false;
    for (auto& filter : filters) {
        if (already_pushed) {
            filters_new.push_back(std::move(filter));
            continue;
        }
        bool can_pushdown = false;
        if (filter->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
            auto& comparison = filter->Cast<BoundComparisonExpression>();
            if (comparison.GetExpressionType() == ExpressionType::COMPARE_EQUAL) {
                bool left_is_scalar = comparison.left->IsFoldable();
                bool right_is_scalar = comparison.right->IsFoldable();
                if (left_is_scalar || right_is_scalar) {
                    auto column_name = comparison.left->ToString();
                    if (column_name == SRC_GID_COLUMN &&
                            edge_info->HasAdjacentListType(graphar::AdjListType::ordered_by_source) &&
                            edge_info->GetAdjacentList(graphar::AdjListType::ordered_by_source)->GetFileType() ==
                                graphar::FileType::PARQUET ||
                        column_name == DST_GID_COLUMN &&
                            edge_info->HasAdjacentListType(graphar::AdjListType::ordered_by_dest) &&
                            edge_info->GetAdjacentList(graphar::AdjListType::ordered_by_dest)->GetFileType() ==
                                graphar::FileType::PARQUET) {
                        can_pushdown = true;
                        const auto vid = std::stoll(comparison.right->ToString());
                        read_bind_data->vid_range = std::make_pair(vid, vid);
                        read_bind_data->filter_column = column_name;
                    }
                }
            }
        }
        if (!can_pushdown) {
            filters_new.push_back(std::move(filter));
        } else {
            already_pushed = true;
        }
    }
    filters = std::move(filters_new);
}
//-------------------------------------------------------------------
// GetFunction
//-------------------------------------------------------------------
TableFunction ReadEdges::GetFunction() {
    TableFunction read_edges("read_edges", {LogicalType::VARCHAR}, Execute, Bind);
    read_edges.init_global = ReadEdges::Init;

    read_edges.named_parameters["src"] = LogicalType::VARCHAR;
    read_edges.named_parameters["dst"] = LogicalType::VARCHAR;
    read_edges.named_parameters["type"] = LogicalType::VARCHAR;

    read_edges.filter_pushdown = false;
    read_edges.projection_pushdown = true;
    read_edges.statistics = ReadEdges::GetStatistics;
    read_edges.pushdown_complex_filter = ReadEdges::PushdownComplexFilter;

    return read_edges;
}
//-------------------------------------------------------------------
// GetScanFunction
//-------------------------------------------------------------------
TableFunction ReadEdges::GetScanFunction() {
    TableFunction read_edges({}, Execute, Bind);
    read_edges.init_global = ReadEdges::Init;

    read_edges.filter_pushdown = false;
    read_edges.projection_pushdown = true;
    read_edges.statistics = ReadEdges::GetStatistics;
    read_edges.pushdown_complex_filter = ReadEdges::PushdownComplexFilter;

    return read_edges;
}
}  // namespace duckdb
