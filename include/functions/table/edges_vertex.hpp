#pragma once

#include <duckdb/common/named_parameter_map.hpp>
#include <duckdb/function/table_function.hpp>

#include <graphar/api/high_level_reader.h>
#include <graphar/graph_info.h>

namespace duckdb {

struct EdgesVertex;
struct EdgesVertexGlobalTableFunctionState;

class EdgesVertexBindData final : public TableFunctionData {
public:
    explicit EdgesVertexBindData(const std::string& file_path) : file_path(file_path) {};

    const std::string& GetFilePath() const { return file_path; }
    void SetFilePath(const std::string& path) { file_path = path; }

    const std::shared_ptr<graphar::GraphInfo>& GetGraphInfo() const { return graph_info; }
    void SetGraphInfo(const std::shared_ptr<graphar::GraphInfo>& info) { graph_info = info; }

    const std::shared_ptr<graphar::EdgeInfo>& GetEdgeInfo() const { return edge_info; }
    void SetEdgeInfo(const std::shared_ptr<graphar::EdgeInfo>& info) { edge_info = info; }

private:
    std::string file_path;
    std::shared_ptr<graphar::GraphInfo> graph_info;
    std::shared_ptr<graphar::EdgeInfo> edge_info;

    friend struct EdgesVertex;
    friend struct EdgesVertexGlobalTableFunctionState;
};

struct EdgesVertexGlobalState {
public:
    EdgesVertexGlobalState(ClientContext& context, const EdgesVertexBindData& bind_data,
                           std::shared_ptr<graphar::EdgeInfo> edge_info, const std::string& prefix, idx_t iter,
                           idx_t end_iter)
        : edge_info(edge_info), prefix(prefix), iter(iter), end_iter(end_iter) {};

    const std::shared_ptr<graphar::EdgeInfo>& GetEdgeInfo() const { return edge_info; }
    const std::string& GetPrefix() const { return prefix; }
    idx_t GetIter() const { return iter; }
    idx_t GetEndIter() const { return end_iter; }
    idx_t GetChunkCount() const { return chunk_count; }

    void IncrementChunkCount() { ++chunk_count; }
    void IncrementIter() { ++iter; }

private:
    std::shared_ptr<graphar::EdgeInfo> edge_info;
    std::string prefix;
    idx_t iter;
    idx_t end_iter;
    idx_t chunk_count = 0;

    friend struct EdgesVertex;
};

struct EdgesVertexGlobalTableFunctionState : public GlobalTableFunctionState {
public:
    EdgesVertexGlobalTableFunctionState(ClientContext& context, const EdgesVertexBindData& bind_data,
                                        std::shared_ptr<graphar::EdgeInfo> edge_info, const std::string& prefix,
                                        idx_t iter, idx_t end_iter)
        : state(context, bind_data, edge_info, prefix, iter, end_iter) {};

    static unique_ptr<GlobalTableFunctionState> Init(ClientContext& context, TableFunctionInitInput& input);

    EdgesVertexGlobalState& GetState() { return state; }

private:
    EdgesVertexGlobalState state;

    friend struct EdgesVertex;
};

struct EdgesVertex {
    static unique_ptr<FunctionData> Bind(ClientContext& context, TableFunctionBindInput& input,
                                         vector<LogicalType>& return_types, vector<string>& names);
    static graphar::Result<vector<std::pair<graphar::IdType, graphar::IdType>>> GetAdjListOffsetOfVertices(
        ClientContext& context, const std::shared_ptr<graphar::EdgeInfo>& edge_info, const std::string& prefix,
        graphar::AdjListType adj_list_type, idx_t start, idx_t end);
    static void Execute(ClientContext& context, TableFunctionInput& data, DataChunk& output);
    static void Register(DatabaseInstance& db);
    static TableFunction GetFunction();
};
}  // namespace duckdb
