#include "functions/table/read_vertices.hpp"

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
#include <graphar/filesystem.h>
#include <graphar/fwd.h>

#include <iostream>

namespace duckdb {
//-------------------------------------------------------------------
// GetBindData
//-------------------------------------------------------------------
void ReadVertices::SetBindData(std::shared_ptr<graphar::GraphInfo> graph_info,
                               std::shared_ptr<graphar::VertexInfo> vertex_info, unique_ptr<ReadBindData>& bind_data) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadVertices::SetBindData");
    ReadBase::SetBindData(graph_info, vertex_info, bind_data, "read_vertices", 1, 0, {GID_COLUMN_INTERNAL});
}
//-------------------------------------------------------------------
// Bind
//-------------------------------------------------------------------
unique_ptr<FunctionData> ReadVertices::Bind(ClientContext& context, TableFunctionBindInput& input,
                                            vector<LogicalType>& return_types, vector<string>& names) {
    bool time_logging = GraphArSettings::is_time_logging(context);

    ScopedTimer t("Bind");

    DUCKDB_GRAPHAR_LOG_DEBUG("ReadVertices::Bind Parse parameters");

    const auto file_path = StringValue::Get(input.inputs[0]);
    const std::string v_type = StringValue::Get(input.named_parameters.at("type"));

    DUCKDB_GRAPHAR_LOG_DEBUG("Get type " + v_type + '\n' + "Load Graph Info and Vertex Info");

    auto bind_data = make_uniq<ReadBindData>();
    auto maybe_graph_info = graphar::GraphInfo::Load(file_path);
    if (maybe_graph_info.has_error()) {
        throw IOException("Failed to load graph info from path: %s", file_path);
    }
    auto graph_info = maybe_graph_info.value();

    auto vertex_info = graph_info->GetVertexInfo(v_type);

    if (!vertex_info) {
        throw BinderException("No vertices of this type");
    }

    DUCKDB_GRAPHAR_LOG_DEBUG("Fill bind data");

    SetBindData(graph_info, vertex_info, bind_data);

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
std::shared_ptr<Reader> ReadVertices::GetReader(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate,
                                                ReadBindData& bind_data, idx_t ind, const std::string& filter_column) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadVertices::GetReader");
    auto vertex_info = *std::get_if<std::shared_ptr<graphar::VertexInfo>>(&bind_data.type_info);
    if (!vertex_info) {
        throw InternalException("Failed to get vertex info");
    }
    const auto& prefix = bind_data.graph_info->GetPrefix();
    if (bind_data.pgs[ind]->GetFileType() == graphar::FileType::PARQUET) {
        DUCKDB_GRAPHAR_LOG_DEBUG("Making duckdb reader");
        return ConvertReader(graphar::DuckVertexPropertyChunkReader::Make(context, gstate.file_reader, vertex_info,
                                                                          bind_data.pgs[ind], prefix));
    } else {
        DUCKDB_GRAPHAR_LOG_DEBUG("Making arrow reader");
        return ConvertReader(
            graphar::DuckVertexPropertyArrowChunkReader::Make(context, vertex_info, bind_data.pgs[ind], prefix));
    }
}
//-------------------------------------------------------------------
// GetStatistics
//-------------------------------------------------------------------
unique_ptr<BaseStatistics> ReadVertices::GetStatistics(ClientContext& context, const FunctionData* bind_data,
                                                       column_t column_index) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadVertices::GetStatistics");
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
    return stats.ToUnique();
}
//-------------------------------------------------------------------
// PushdownComplexFilter
//-------------------------------------------------------------------
void ReadVertices::PushdownComplexFilter(ClientContext& context, LogicalGet& get, FunctionData* bind_data,
                                         vector<unique_ptr<Expression>>& filters) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadVertices::PushdownComplexFilter");
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
                    if (column_name == GID_COLUMN_INTERNAL) {
                        can_pushdown = true;
                        const auto vid = std::stoll(comparison.right->ToString());
                        read_bind_data->vid_range = std::make_pair(vid, vid);
                        read_bind_data->filter_column = column_name;
                    }
                }
            }
        }
        if (!can_pushdown) {
            already_pushed = true;
            filters_new.push_back(std::move(filter));
        }
    }
    filters = std::move(filters_new);
}
//-------------------------------------------------------------------
// GetFunction
//-------------------------------------------------------------------
TableFunction ReadVertices::GetFunction() {
    TableFunction read_vertices("read_vertices", {LogicalType::VARCHAR}, Execute, Bind);
    read_vertices.init_global = ReadVertices::Init;

    read_vertices.named_parameters["type"] = LogicalType::VARCHAR;

    read_vertices.filter_pushdown = false;
    read_vertices.projection_pushdown = true;
    read_vertices.statistics = ReadVertices::GetStatistics;
    read_vertices.pushdown_complex_filter = ReadVertices::PushdownComplexFilter;

    return read_vertices;
}
//-------------------------------------------------------------------
// GetScanFunction
//-------------------------------------------------------------------
TableFunction ReadVertices::GetScanFunction() {
    TableFunction read_vertices({}, Execute, Bind);
    read_vertices.init_global = ReadVertices::Init;

    read_vertices.filter_pushdown = false;
    read_vertices.projection_pushdown = true;
    read_vertices.statistics = ReadVertices::GetStatistics;
    read_vertices.pushdown_complex_filter = ReadVertices::PushdownComplexFilter;

    return read_vertices;
}
}  // namespace duckdb
