#include "functions/table/read_edges.hpp"
#include "utils/benchmark.hpp"
#include "utils/func.hpp"

#include "duckdb/common/named_parameter_map.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"

#include "graphar/api/arrow_reader.h"
#include "graphar/api/high_level_reader.h"
#include "graphar/arrow/chunk_reader.h"
#include "graphar/expression.h"
#include "graphar/fwd.h"

#include "arrow/c/bridge.h"

#include <cassert>
#include <iostream>

namespace duckdb {
//-------------------------------------------------------------------
// Bind
//-------------------------------------------------------------------
unique_ptr<FunctionData> ReadEdges::Bind(ClientContext& context, TableFunctionBindInput& input,
                                         vector<LogicalType>& return_types, vector<string>& names) {
    bool time_logging = GraphArSettings::is_time_logging(context);

    ScopedTimer t("Bind");

    LOG_TRACE("ReadEdges::Bind\nParse parameters");

    const auto file_path = StringValue::Get(input.inputs[0]);
    const std::string src_type = StringValue::Get(input.named_parameters.at("src"));
    const std::string dst_type = StringValue::Get(input.named_parameters.at("dst"));
    const std::string e_type = StringValue::Get(input.named_parameters.at("type"));

    LOG_DEBUG(src_type + "--" + e_type + "->" + dst_type + "\nLoad Graph Info and Edge Info");

    auto bind_data = make_uniq<ReadBindData>();
    bind_data->params = {src_type, e_type, dst_type};
    LOG_DEBUG("file path " + file_path);
    bind_data->graph_info = graphar::GraphInfo::Load(file_path).value();

    auto edge_info = bind_data->graph_info->GetEdgeInfo(src_type, e_type, dst_type);
    if (!edge_info) {
        throw BinderException("Edges of this type are not found");
    }

    LOG_DEBUG("Create BindData");

    LOG_DEBUG("Set types and names");

    bind_data->pgs = edge_info->GetPropertyGroups();
    LOG_DEBUG("pgs size " + std::to_string(bind_data->pgs.size()));
    bind_data->prop_types.resize(bind_data->pgs.size() + 1);
    bind_data->prop_names.resize(bind_data->prop_types.size());

    return_types.push_back(LogicalTypeId::BIGINT);
    names.push_back(SRC_GID_COLUMN);
    bind_data->prop_types[0].push_back("int64");
    bind_data->flatten_prop_types.push_back("int64");
    bind_data->prop_names[0].push_back(SRC_GID_COLUMN);

    return_types.push_back(LogicalTypeId::BIGINT);
    names.push_back(DST_GID_COLUMN);
    bind_data->prop_types[0].push_back("int64");
    bind_data->flatten_prop_types.push_back("int64");
    bind_data->prop_names[0].push_back(DST_GID_COLUMN);

    for (idx_t i = 0; i < bind_data->pgs.size(); ++i) {
        for (auto p : bind_data->pgs[i]->GetProperties()) {
            auto type_name = p.type->ToTypeName();
            return_types.push_back(GraphArFunctions::graphArT2duckT(type_name));
            names.push_back(p.name);
            bind_data->prop_types[i + 1].push_back(p.type->ToTypeName());
            bind_data->flatten_prop_types.push_back(p.type->ToTypeName());
            bind_data->prop_names[i + 1].push_back(p.name);
        }
    }

    bind_data->function_name = "read_edges";
    bind_data->flatten_prop_names = names;

    LOG_DEBUG("Bind finish");
    if (time_logging) {
        t.print();
    }

    return std::move(bind_data);
}
//-------------------------------------------------------------------
// GetReader
//-------------------------------------------------------------------
std::shared_ptr<Reader> ReadEdges::GetReader(ReadBaseGlobalTableFunctionState& gstate, ReadBindData& bind_data,
                                             idx_t ind, const std::string& filter_value,
                                             const std::string& filter_column, const std::string& filter_type) {
    graphar::AdjListType adj_list_type;
    if (filter_column == "" or filter_column == SRC_GID_COLUMN) {
        adj_list_type = graphar::AdjListType::ordered_by_source;
    } else if (filter_column == DST_GID_COLUMN) {
        adj_list_type = graphar::AdjListType::ordered_by_dest;
    } else {
        throw NotImplementedException("Only src and dst filters are supported");
    }
    if (ind == 0) {
        auto maybe_reader = graphar::AdjListArrowChunkReader::Make(
            bind_data.graph_info, bind_data.params[0], bind_data.params[1], bind_data.params[2], adj_list_type);
        assert(maybe_reader.status().ok());
        Reader result = *maybe_reader.value();
        return std::make_shared<Reader>(std::move(result));
    }
    graphar::Result<std::shared_ptr<graphar::AdjListPropertyArrowChunkReader>> maybe_reader;
    maybe_reader =
        graphar::AdjListPropertyArrowChunkReader::Make(bind_data.graph_info, bind_data.params[0], bind_data.params[1],
                                                       bind_data.params[2], bind_data.pgs[ind - 1], adj_list_type);
    assert(maybe_reader.status().ok());
    Reader result = *maybe_reader.value();
    return std::make_shared<Reader>(std::move(result));
}
//-------------------------------------------------------------------
// SetFilter
//-------------------------------------------------------------------
void ReadEdges::SetFilter(ReadBaseGlobalTableFunctionState& gstate, ReadBindData& bind_data, std::string& filter_value,
                          std::string& filter_column, std::string& filter_type) {
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
    graphar::IdType vid = std::stoll(filter_value);
    offset_reader->seek(vid);
    for (idx_t i = 0; i < gstate.readers.size(); ++i) {
        seek_chunk_index(*gstate.readers[i], offset_reader->GetChunkIndex());
    }
    auto offset_arr = offset_reader->GetChunk().value();
    gstate.filter_range.first = GetInt64Value(offset_arr, 0);
    gstate.filter_range.second = GetInt64Value(offset_arr, 1);
}
//-------------------------------------------------------------------
// Register
//-------------------------------------------------------------------
TableFunction ReadEdges::GetFunction() {
    TableFunction read_edges("read_edges", {LogicalType::VARCHAR}, Execute, Bind);
    read_edges.init_global = ReadEdges::Init;

    read_edges.named_parameters["src"] = LogicalType::VARCHAR;
    read_edges.named_parameters["dst"] = LogicalType::VARCHAR;
    read_edges.named_parameters["type"] = LogicalType::VARCHAR;
    read_edges.filter_pushdown = true;
    // read_edges.filter_pushdown = false;

    return read_edges;
}
}  // namespace duckdb
