#include "functions/table/edges_vertex.hpp"

#include <iostream>

#include "duckdb/common/named_parameter_map.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "graphar/api/high_level_reader.h"
#include "graphar/status.h"
#include "utils/benchmark.hpp"
#include "utils/func.hpp"
#include "utils/global_log_manager.hpp"

namespace duckdb {
//-------------------------------------------------------------------
// Bind
//-------------------------------------------------------------------
unique_ptr<FunctionData> EdgesVertex::Bind(ClientContext& context, TableFunctionBindInput& input,
                                           vector<LogicalType>& return_types, vector<string>& names) {
    bool time_logging = GraphArSettings::is_time_logging(context);

    ScopedTimer t("Bind");

    LOG_TRACE("EdgesVertex::Bind");
    LOG_DEBUG("Parse parameters");
    const auto file_path = StringValue::Get(input.inputs[0]);

    LOG_DEBUG("Load Graph Info");

    auto graph_info = graphar::GraphInfo::Load(file_path).value();

    LOG_DEBUG("Load Edge Info");

    auto yaml_content = GetYamlContent(file_path);
    auto edge_info = graphar::EdgeInfo::Load(yaml_content).value();
    if (!edge_info) {
        throw BinderException("No edge of this type");
    }

    LOG_DEBUG("Create BindData");

    auto bind_data = make_uniq<EdgesVertexBindData>(file_path);
    bind_data->SetGraphInfo(graph_info);
    bind_data->SetEdgeInfo(edge_info);

    return_types.push_back(LogicalType::BIGINT);
    names.push_back("degree");
    return_types.push_back(LogicalTypeId::BIGINT);
    names.push_back(GID_COLUMN);

    LOG_DEBUG("Bind finish");
    if (time_logging) {
        t.print();
    }

    return std::move(bind_data);
}
//-------------------------------------------------------------------
// State Init
//-------------------------------------------------------------------
unique_ptr<GlobalTableFunctionState> EdgesVertexGlobalTableFunctionState::Init(ClientContext& context,
                                                                               TableFunctionInitInput& input) {
    bool time_logging = GraphArSettings::is_time_logging(context);

    ScopedTimer t("StateInit");

    LOG_TRACE("EdgesVertexGlobalTableFunctionState::Init");
    LOG_DEBUG("Cast BindData");

    auto bind_data = input.bind_data->Cast<EdgesVertexBindData>();

    if (time_logging) {
        t.print("cast");
    }

    if (time_logging) {
        t.print("edges");
    }

    auto prefix = GetDirectory(bind_data.GetFilePath());
    auto vertex_count = GetVertexCount(bind_data.GetEdgeInfo(), prefix);
    idx_t iter = 0, end_iter = vertex_count;

    if (input.filters) {
        LOG_DEBUG("Found filters");

        if (input.filters->filters.size() > 1) {
            throw NotImplementedException("Multiple filters are not supported");
        }
        auto filter_id = input.filters->filters.begin()->first;
        auto filter_index = input.column_ids[filter_id];
        auto& filter = input.filters->filters.begin()->second;
        if (filter->filter_type != TableFilterType::CONSTANT_COMPARISON) {
            throw NotImplementedException("Only constant filters are supported");
        }
        auto filter_expr = filter->ToString(" ");
        if (filter_expr[1] != '=') {
            throw NotImplementedException("Only equality filters are supported");
        }

        auto filter_value = filter_expr.substr(2);

        if (filter_index + 1 == input.column_ids.size()) {
            LOG_DEBUG("Filter by gid");
            int vid = std::stoi(filter_value);

            iter = vid;
            end_iter = vid + 1;
        } else {
            throw NotImplementedException("Filter by property is not supported");
        }
    }

    LOG_DEBUG("Init finish");
    if (time_logging) {
        t.print();
    }

    return make_uniq<EdgesVertexGlobalTableFunctionState>(context, bind_data, bind_data.GetEdgeInfo(), prefix, iter,
                                                          end_iter);
}
//-------------------------------------------------------------------
// GetAdjListOffsetOfVertices
//-------------------------------------------------------------------
graphar::Result<vector<std::pair<graphar::IdType, graphar::IdType>>> EdgesVertex::GetAdjListOffsetOfVertices(
    ClientContext& context, const std::shared_ptr<graphar::EdgeInfo>& edge_info, const std::string& prefix,
    graphar::AdjListType adj_list_type, idx_t start, idx_t end) {
    bool time_logging = GraphArSettings::is_time_logging(context);

    ScopedTimer t("GetAdjListOffsetOfVertices");

    if (start >= end) {
        return vector<std::pair<graphar::IdType, graphar::IdType>>(0);
    }
    graphar::IdType vertex_chunk_size;
    if (adj_list_type == graphar::AdjListType::ordered_by_source) {
        vertex_chunk_size = edge_info->GetSrcChunkSize();
    } else if (adj_list_type == graphar::AdjListType::ordered_by_dest) {
        vertex_chunk_size = edge_info->GetDstChunkSize();
    } else {
        return graphar::Status::Invalid(
            "The adj list type has to be ordered_by_source or ordered_by_dest, but "
            "got ",
            std::string(AdjListTypeToString(adj_list_type)));
    }

    graphar::IdType offset_chunk_index = start / vertex_chunk_size;
    graphar::IdType offset_in_file = start % vertex_chunk_size;
    GAR_ASSIGN_OR_RAISE(auto offset_file_path, edge_info->GetAdjListOffsetFilePath(offset_chunk_index, adj_list_type));
    std::string out_prefix;
    GAR_ASSIGN_OR_RAISE(auto fs, graphar::FileSystemFromUriOrPath(prefix, &out_prefix));
    auto adjacent_list = edge_info->GetAdjacentList(adj_list_type);
    if (adjacent_list == nullptr) {
        return graphar::Status::Invalid("The adjacent list is not set for adj list type ",
                                        std::string(AdjListTypeToString(adj_list_type)));
    }
    auto file_type = adjacent_list->GetFileType();
    std::string path = out_prefix + offset_file_path;
    LOG_DEBUG("Opening table");
    GAR_ASSIGN_OR_RAISE(auto table, fs->ReadFileToTable(path, file_type));
    idx_t len = end - start;
    auto result = vector<std::pair<graphar::IdType, graphar::IdType>>(len);
    auto slice = table->column(0)->Slice(offset_in_file, len + 1);
    auto chunks_num = slice->num_chunks();
    auto chunk_size = slice->chunk(0)->length();

    vector<std::shared_ptr<arrow::NumericArray<arrow::Int64Type>>> arrays(chunks_num);
    for (idx_t i = 0; i < chunks_num; ++i) {
        arrays[i] = std::static_pointer_cast<arrow::NumericArray<arrow::Int64Type>>(slice->chunk(i));
    }

    for (idx_t i = 0; i < chunks_num; ++i) {
        for (idx_t j = 0; j < arrays[i]->length(); ++j) {
            idx_t nxt = 0;
            if (j < arrays[i]->length() - 1) {
                nxt = arrays[i]->Value(j + 1);
            } else if (i < chunks_num - 1) {
                nxt = arrays[i + 1]->Value(0);
            } else {
                break;
            }
            auto np =
                std::make_pair(static_cast<graphar::IdType>(arrays[i]->Value(j)), static_cast<graphar::IdType>(nxt));
            result[i * chunk_size + j] = np;
        }
    }
    return result;
}
//-------------------------------------------------------------------
// Execute
//-------------------------------------------------------------------
inline void EdgesVertex::Execute(ClientContext& context, TableFunctionInput& input, DataChunk& output) {
    bool time_logging = GraphArSettings::is_time_logging(context);

    ScopedTimer t("Execute");

    LOG_TRACE("EdgesVertex::Execute");
    LOG_DEBUG("Cast Global state");

    EdgesVertexGlobalState& gstate = input.global_state->Cast<EdgesVertexGlobalTableFunctionState>().GetState();

    LOG_DEBUG("Chunk " + std::to_string(gstate.GetChunkCount()) + ": Begin iteration");
    idx_t start = gstate.GetIter();
    idx_t end = gstate.GetIter() + STANDARD_VECTOR_SIZE;
    if (end > gstate.GetEndIter()) {
        end = gstate.GetEndIter();
    }
    LOG_DEBUG("start: " + std::to_string(start) + " end: " + std::to_string(end));
    LOG_DEBUG("Calling GetAdjListOffsetOfVertices");
    vector<std::pair<graphar::IdType, graphar::IdType>> result =
        GetAdjListOffsetOfVertices(context, gstate.GetEdgeInfo(), gstate.GetPrefix(),
                                   graphar::AdjListType::ordered_by_source, start, end)
            .value();
    LOG_DEBUG("Filling output");
    for (; gstate.GetIter() < end; gstate.IncrementIter()) {
        auto i = gstate.GetIter() - start;
        output.SetValue(0, i, result[i].second - result[i].first);
        output.SetValue(1, i, static_cast<int64_t>(gstate.GetIter()));
    }
    if (end <= start) {
        output.SetCapacity(0);
        output.SetCardinality(0);
    } else {
        output.SetCapacity(end - start);
        output.SetCardinality(end - start);
    }

    LOG_DEBUG("Chunk " + std::to_string(gstate.GetChunkCount()) + ": Finish iteration");

    if (time_logging) {
        t.print();
    }
    gstate.IncrementChunkCount();
}
//-------------------------------------------------------------------
// Register
//-------------------------------------------------------------------
TableFunction EdgesVertex::GetFunction() {
    TableFunction edges_vertex("edges_vertex", {LogicalType::VARCHAR}, Execute, Bind);
    edges_vertex.init_global = EdgesVertexGlobalTableFunctionState::Init;

    // edges_vertex.named_parameters["type"] = LogicalType::VARCHAR;
    edges_vertex.filter_pushdown = true;

    return edges_vertex;
}

void EdgesVertex::Register(DatabaseInstance& db) { ExtensionUtil::RegisterFunction(db, GetFunction()); }
}  // namespace duckdb
