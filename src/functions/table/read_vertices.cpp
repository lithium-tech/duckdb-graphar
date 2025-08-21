#include "functions/table/read_vertices.hpp"

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
#include <graphar/filesystem.h>
#include <graphar/fwd.h>

#include <cassert>
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
    auto graph_info = graphar::GraphInfo::Load(file_path).value();

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
                                                idx_t ind, const std::string& filter_value,
                                                const std::string& filter_column, const std::string& filter_type) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadVertices::GetReader");
    auto maybe_reader =
        graphar::VertexPropertyArrowChunkReader::Make(bind_data.graph_info, bind_data.params[0], bind_data.pgs[ind]);
    assert(maybe_reader.status().ok());
    Reader result = *maybe_reader.value();
    return std::make_shared<Reader>(std::move(result));
}
//-------------------------------------------------------------------
// SetFilter
//-------------------------------------------------------------------
void ReadVertices::SetFilter(ReadBaseGlobalTableFunctionState& gstate, ReadBindData& bind_data,
                             std::string& filter_value, std::string& filter_column, std::string& filter_type) {
    if (filter_column == "") {
        return;
    }
    if (filter_column == GID_COLUMN_INTERNAL) {
        graphar::IdType vid = std::stoll(filter_value);
        for (idx_t i = 0; i < gstate.readers.size(); ++i) {
            seek_vid(*gstate.readers[i], vid, filter_column);
        }
        gstate.filter_range.first = 0;
        gstate.filter_range.second = 1;
    } else {
        auto g_filter = GraphArFunctions::GetFilter(filter_type, filter_value, filter_column);
        for (idx_t i = 0; i < gstate.readers.size(); ++i) {
            Filter(*gstate.readers[i], g_filter);
        }
    }
}
//-------------------------------------------------------------------
// GetFunction
//-------------------------------------------------------------------
TableFunction ReadVertices::GetFunction() {
    TableFunction read_vertices("read_vertices", {LogicalType::VARCHAR}, Execute, Bind);
    read_vertices.init_global = ReadVertices::Init;

    read_vertices.named_parameters["type"] = LogicalType::VARCHAR;
    read_vertices.filter_pushdown = true;
    read_vertices.projection_pushdown = true;

    return read_vertices;
}
//-------------------------------------------------------------------
// GetScanFunction
//-------------------------------------------------------------------
TableFunction ReadVertices::GetScanFunction() {
    TableFunction read_vertices({}, Execute, Bind);
    read_vertices.init_global = ReadVertices::Init;

    read_vertices.filter_pushdown = true;
    read_vertices.projection_pushdown = true;

    return read_vertices;
}
}  // namespace duckdb
