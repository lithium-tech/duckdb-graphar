#include "functions/table/read_vertices.hpp"

#include <cassert>
#include <iostream>

#include "arrow/c/bridge.h"
#include "duckdb/common/named_parameter_map.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "graphar/api/arrow_reader.h"
#include "graphar/api/high_level_reader.h"
#include "graphar/arrow/chunk_reader.h"
#include "graphar/expression.h"
#include "graphar/fwd.h"
#include "utils/benchmark.hpp"
#include "utils/func.hpp"

namespace duckdb {
//-------------------------------------------------------------------
// Bind
//-------------------------------------------------------------------
unique_ptr<FunctionData> ReadVertices::Bind(ClientContext& context, TableFunctionBindInput& input,
                                            vector<LogicalType>& return_types, vector<string>& names) {
    bool time_logging = GraphArSettings::is_time_logging(context);

    ScopedTimer t("Bind");

    LOG_DEBUG("ReadVertices::Bind\nParse parameters");

    const auto file_path = StringValue::Get(input.inputs[0]);
    const std::string v_type = StringValue::Get(input.named_parameters.at("type"));

    LOG_DEBUG("Get type " + v_type + '\n' + "Load Graph Info and Vertex Info");

    auto bind_data = make_uniq<ReadBindData>();
    bind_data->params = {v_type};
    std::cout << "file path " << file_path << std::endl;
    bind_data->graph_info = graphar::GraphInfo::Load(file_path).value();

    auto vertex_info = bind_data->graph_info->GetVertexInfo(v_type);

    if (!vertex_info) {
        throw BinderException("No vertices of this type");
    }

    LOG_DEBUG("Set types and names");

    bind_data->pgs = vertex_info->GetPropertyGroups();
    LOG_DEBUG("pgs size " + std::to_string(bind_data->pgs.size()));
    bind_data->prop_types.resize(bind_data->pgs.size());
    bind_data->prop_names.resize(bind_data->prop_types.size());

    return_types.push_back(LogicalType::BIGINT);
    names.push_back(GID_COLUMN_INTERNAL);
    bind_data->prop_types[0].push_back("int64");
    bind_data->flatten_prop_types.push_back("int64");
    bind_data->prop_names[0].push_back(GID_COLUMN_INTERNAL);

    for (idx_t i = 0; i < bind_data->pgs.size(); ++i) {
        for (auto p : bind_data->pgs[i]->GetProperties()) {
            auto type_name = p.type->ToTypeName();
            return_types.push_back(GraphArFunctions::graphArT2duckT(type_name));
            names.push_back(p.name);
            bind_data->prop_types[i].push_back(p.type->ToTypeName());
            bind_data->flatten_prop_types.push_back(p.type->ToTypeName());
            bind_data->prop_names[i].push_back(p.name);
        }
    }

    bind_data->function_name = "read_vertices";
    bind_data->flatten_prop_names = names;
    bind_data->columns_to_remove = 1;

    LOG_DEBUG("Bind finish");
    if (time_logging) {
        t.print();
    }

    return std::move(bind_data);
}
//-------------------------------------------------------------------
// GetReader
//-------------------------------------------------------------------
std::shared_ptr<Reader> ReadVertices::GetReader(ReadBaseGlobalTableFunctionState& gstate, ReadBindData& bind_data,
                                                idx_t ind, const std::string& filter_value,
                                                const std::string& filter_column, const std::string& filter_type) {
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
        gstate.filter_range.first = vid;
        gstate.filter_range.second = vid + 1;
    } else {
        auto g_filter = GraphArFunctions::GetFilter(filter_type, filter_value, filter_column);
        for (idx_t i = 0; i < gstate.readers.size(); ++i) {
            Filter(*gstate.readers[i], g_filter);
        }
    }
}
//-------------------------------------------------------------------
// Register
//-------------------------------------------------------------------
TableFunction ReadVertices::GetFunction() {
    TableFunction read_vertices("read_vertices", {LogicalType::VARCHAR}, Execute, Bind);
    read_vertices.init_global = ReadVertices::Init;

    read_vertices.named_parameters["type"] = LogicalType::VARCHAR;
    read_vertices.filter_pushdown = true;
    // read_vertices.filter_pushdown = false;

    return read_vertices;
}
}  // namespace duckdb
