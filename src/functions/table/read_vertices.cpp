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
void ReadVertices::SetBindData(std::shared_ptr<graphar::GraphInfo> graph_info, const graphar::VertexInfo& vertex_info,
                               unique_ptr<ReadBindData>& bind_data) {
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

    SetBindData(graph_info, *vertex_info, bind_data);

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
std::shared_ptr<Reader> ReadVertices::GetReader(ReadBaseGlobalTableFunctionState& gstate, ReadBindData& bind_data,
                                                idx_t ind, const std::string& filter_column) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadVertices::GetReader");
    auto maybe_reader =
        graphar::VertexPropertyArrowChunkReader::Make(bind_data.graph_info, bind_data.params[0], bind_data.pgs[ind]);
    if (maybe_reader.has_error()) {
        throw std::runtime_error("Failed to create vertex property reader: " + maybe_reader.status().message());
    }
    Reader result = *maybe_reader.value();
    return std::make_shared<Reader>(std::move(result));
}
//-------------------------------------------------------------------
// SetFilter
//-------------------------------------------------------------------
void ReadVertices::SetFilter(ReadBaseGlobalTableFunctionState& gstate, ReadBindData& bind_data,
                             const std::pair<graphar::IdType, graphar::IdType>& vid_range,
                             const std::string& filter_column) {
    if (filter_column == "") {
        return;
    }
    if (filter_column == GID_COLUMN_INTERNAL) {
        for (idx_t i = 0; i < gstate.readers.size(); ++i) {
            seek_vid(*gstate.readers[i], vid_range.first, filter_column);
        }
        gstate.filter_range.first = 0;
        gstate.filter_range.second = vid_range.second - vid_range.second + 1;
    } else {
        throw BinderException("Filter on vertex property is not supported by this method");
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
    auto v_type = (column_name == SRC_GID_COLUMN) ? read_bind_data.GetParams()[0] : read_bind_data.GetParams()[2];
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
                        auto read_bind_data = dynamic_cast<ReadBindData*>(bind_data);
                        read_bind_data->vid_range = std::make_pair(std::stoll(comparison.right->ToString()),
                                                                   std::stoll(comparison.right->ToString()));
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
