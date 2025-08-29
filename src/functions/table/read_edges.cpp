#include "functions/table/read_edges.hpp"

#include "utils/benchmark.hpp"
#include "utils/func.hpp"

#include <arrow/c/bridge.h>

#include <duckdb/common/named_parameter_map.hpp>
#include <duckdb/function/table/arrow.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/extension_util.hpp>

#include <graphar/api/arrow_reader.h>
#include <graphar/api/high_level_reader.h>
#include <graphar/arrow/chunk_reader.h>
#include <graphar/expression.h>
#include <graphar/fwd.h>

#include <cassert>
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

    DUCKDB_GRAPHAR_LOG_DEBUG(src_type + "--" + e_type + "->" + dst_type + " Load Graph Info and Edge Info");

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
                                             idx_t ind, const std::string& filter_value,
                                             const std::string& filter_column, const std::string& filter_type) {
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
        auto maybe_reader = graphar::AdjListChunkInfoReader::Make(
            bind_data.graph_info, bind_data.params[0], bind_data.params[1], bind_data.params[2], adj_list_type);
        assert(maybe_reader.status().ok());
        Reader result = *maybe_reader.value();
        DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::GetReader: returning...");
        return std::make_shared<Reader>(std::move(result));
    }
    DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::GetReader: making property reader...");
    auto maybe_reader =
        graphar::AdjListPropertyChunkInfoReader::Make(bind_data.graph_info, bind_data.params[0], bind_data.params[1],
                                                      bind_data.params[2], bind_data.pgs[ind - 1], adj_list_type);
    assert(maybe_reader.status().ok());
    Reader result = *maybe_reader.value();
    DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::GetReader: returning...");
    return std::make_shared<Reader>(std::move(result));
}
//-------------------------------------------------------------------
// SetFilter
//-------------------------------------------------------------------
void ReadEdges::SetFilter(ReadBaseGlobalTableFunctionState& gstate, ReadBindData& bind_data, std::string& filter_value,
                          std::string& filter_column, std::string& filter_type) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::SetFilter");
    if (filter_column == "") {
        return;
    } else if (filter_column != SRC_GID_COLUMN && filter_column != DST_GID_COLUMN) {
        throw NotImplementedException("Only src and dst filters are supported");
    }
    ScopedTimer t("SetFilter");
    graphar::IdType vid = std::stoll(filter_value);
    int64_t vertex_num = 0;
    if (filter_column == SRC_GID_COLUMN) {
        vertex_num = GraphArFunctions::GetVertexNum(bind_data.graph_info, bind_data.params[0]);
    } else {
        vertex_num = GraphArFunctions::GetVertexNum(bind_data.graph_info, bind_data.params[2]);
    }
    if (vid < 0 or vid >= vertex_num) {
        throw BinderException("Vertex id is out of range");
    }
    t.print("vid check");
    for (idx_t i = 0; i < gstate.readers.size(); ++i) {
        seek_vid(*gstate.readers[i], vid, filter_column);
        t.print("seek_vid");
    }
    DUCKDB_GRAPHAR_LOG_TRACE("ReadEdges::SetFilter: finished");
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
    read_edges.filter_pushdown = true;
    read_edges.projection_pushdown = true;

    return read_edges;
}
//-------------------------------------------------------------------
// GetScanFunction
//-------------------------------------------------------------------
TableFunction ReadEdges::GetScanFunction() {
    TableFunction read_edges({}, Execute, Bind);
    read_edges.init_global = ReadEdges::Init;

    read_edges.filter_pushdown = true;
    read_edges.projection_pushdown = true;

    return read_edges;
}
}  // namespace duckdb
