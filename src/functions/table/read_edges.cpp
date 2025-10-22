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
void ReadEdges::SetBindData(std::shared_ptr<graphar::GraphInfo> graph_info, const graphar::EdgeInfo& edge_info,
                            unique_ptr<ReadBindData>& bind_data) {
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

    SetBindData(graph_info, *edge_info, bind_data);

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
std::shared_ptr<Reader> ReadEdges::GetReader(ReadBaseGlobalTableFunctionState& gstate, ReadBindData& bind_data,
                                             idx_t ind, const std::string& filter_column) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::GetReader");
    graphar::AdjListType adj_list_type;
    if (filter_column == "" or filter_column == SRC_GID_COLUMN) {
        adj_list_type = graphar::AdjListType::ordered_by_source;
    } else if (filter_column == DST_GID_COLUMN) {
        adj_list_type = graphar::AdjListType::ordered_by_dest;
    } else {
        throw NotImplementedException("Only src and dst filters are supported");
    }
    if (ind == 0) {
        DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::GetReader: making src and dst reader...");
        auto maybe_reader = graphar::AdjListArrowChunkReader::Make(
            bind_data.graph_info, bind_data.params[0], bind_data.params[1], bind_data.params[2], adj_list_type);
        if (maybe_reader.has_error()) {
            throw std::runtime_error("Failed to make adj list reader: " + maybe_reader.error().message());
        }
        Reader result = *maybe_reader.value();
        DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::GetReader: returning...");
        return std::make_shared<Reader>(std::move(result));
    }
    DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::GetReader: making property reader...");
    auto maybe_reader =
        graphar::AdjListPropertyArrowChunkReader::Make(bind_data.graph_info, bind_data.params[0], bind_data.params[1],
                                                       bind_data.params[2], bind_data.pgs[ind - 1], adj_list_type);
    if (maybe_reader.has_error()) {
        throw std::runtime_error("Failed to make adj list property reader: " + maybe_reader.error().message());
    }
    Reader result = *maybe_reader.value();
    DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::GetReader: returning...");
    return std::make_shared<Reader>(std::move(result));
}
//-------------------------------------------------------------------
// SetFilter
//-------------------------------------------------------------------
int64_t get_distance(int64_t vid_from_offset, int64_t vid_to_offset, int64_t vid_from_chunk_index,
                     int64_t vid_to_chunk_index, int64_t chunk_size) {
    if (vid_from_chunk_index == vid_to_chunk_index) {
        return vid_to_offset - vid_from_offset;
    }
    return chunk_size - vid_from_offset + vid_to_offset + (vid_to_chunk_index - vid_from_chunk_index - 1) * chunk_size;
}

void ReadEdges::SetFilter(ReadBaseGlobalTableFunctionState& gstate, ReadBindData& bind_data,
                          const std::pair<graphar::IdType, graphar::IdType>& vid_range,
                          const std::string& filter_column) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::SetFilter");
    if (filter_column == "") {
        return;
    }
    auto edge_info = bind_data.graph_info->GetEdgeInfo(bind_data.params[0], bind_data.params[1], bind_data.params[2]);
    std::shared_ptr<graphar::AdjListOffsetArrowChunkReader> offset_reader = nullptr;
    if (filter_column == SRC_GID_COLUMN) {
        offset_reader =
            graphar::AdjListOffsetArrowChunkReader::Make(bind_data.graph_info, bind_data.params[0], bind_data.params[1],
                                                         bind_data.params[2], graphar::AdjListType::ordered_by_source)
                .value();
    } else if (filter_column == DST_GID_COLUMN) {
        offset_reader =
            graphar::AdjListOffsetArrowChunkReader::Make(bind_data.graph_info, bind_data.params[0], bind_data.params[1],
                                                         bind_data.params[2], graphar::AdjListType::ordered_by_dest)
                .value();
    } else {
        throw NotImplementedException("Only src and dst filters are supported");
    }
    for (idx_t i = 0; i < gstate.readers.size(); ++i) {
        seek_vid(*gstate.readers[i], vid_range.first, filter_column);
    }
    offset_reader->seek(vid_range.second);
    auto offset_arr = offset_reader->GetChunk().value();
    auto vid_to_offset = GetInt64Value(offset_arr, 1);
    auto vid_to_chunk_index = offset_reader->GetChunkIndex();
    if (vid_range.first != vid_range.second) {
        offset_reader->seek(vid_range.first);
        offset_arr = offset_reader->GetChunk().value();
    }
    auto vid_from_offset = GetInt64Value(offset_arr, 0);
    auto vid_from_chunk_index = offset_reader->GetChunkIndex();
    int64_t distance = vid_to_offset - vid_from_offset;
    auto now = vid_from_chunk_index;
    while (now < vid_to_chunk_index) {
        offset_arr = offset_reader->GetChunk().value();
        distance += GetInt64Value(offset_arr, offset_arr->length() - 1);
        now++;
    }
    gstate.filter_range.first = 0;
    gstate.filter_range.second = distance;
    DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::SetFilter: finished");
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
    auto v_type = (column_name == SRC_GID_COLUMN) ? read_bind_data.GetParams()[0] : read_bind_data.GetParams()[2];
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
                    if (column_name == SRC_GID_COLUMN || column_name == DST_GID_COLUMN) {
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
