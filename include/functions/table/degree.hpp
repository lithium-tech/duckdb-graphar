#pragma once

#include <arrow/api.h>

#include <duckdb/function/table_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>

#include <graphar/api/high_level_reader.h>
#include <graphar/graph_info.h>

#include <string>
#include <vector>

namespace duckdb {

struct Degree;

class DegreeBindData final : public TableFunctionData {
public:
    std::string file_path;
    std::shared_ptr<graphar::GraphInfo> graph_info;
    std::shared_ptr<graphar::EdgeInfo> edge_info;
    int64_t vertex_count = 0;
    bool has_src = false;
    bool has_dst = false;
    std::vector<std::pair<int64_t, int64_t>> vid_ranges;
};

struct DegreeOffsetCache {
    graphar::IdType chunk_index = -1;
    std::shared_ptr<arrow::ChunkedArray> column;
};

struct DegreeGlobalState {
    std::shared_ptr<graphar::EdgeInfo> edge_info;
    std::string prefix;
    int64_t vertex_count = 0;
    bool has_src = false;
    bool has_dst = false;
    bool read_src = false;
    bool read_dst = false;
    std::vector<std::pair<int64_t, int64_t>> ranges;
    // projection[i] = logical output column (0=vid, 1=out_degree, 2=in_degree)
    // for output.data[i].
    std::vector<int64_t> projection;
    size_t cur_range = 0;
    int64_t iter = 0;
    int64_t end_iter = 0;
    idx_t chunk_count = 0;
    DegreeOffsetCache src_cache;
    DegreeOffsetCache dst_cache;
};

struct DegreeGlobalTableFunctionState : public GlobalTableFunctionState {
    static unique_ptr<GlobalTableFunctionState> Init(ClientContext& context, TableFunctionInitInput& input);

    DegreeGlobalState& GetState() { return state; }

private:
    DegreeGlobalState state;

    friend struct Degree;
};

struct Degree {
    static unique_ptr<FunctionData> Bind(ClientContext& context, TableFunctionBindInput& input,
                                         vector<LogicalType>& return_types, vector<Identifier>& names);
    static void Execute(ClientContext& context, TableFunctionInput& input, DataChunk& output);
    static void PushdownComplexFilter(ClientContext& context, LogicalGet& get, FunctionData* bind_data,
                                      vector<unique_ptr<Expression>>& filters);

    static std::shared_ptr<arrow::ChunkedArray> GetOffsetColumn(DegreeGlobalState& state,
                                                                graphar::AdjListType adj_list_type,
                                                                graphar::IdType vid);
    static std::vector<int64_t> ReadDegrees(DegreeGlobalState& state, graphar::AdjListType adj_list_type,
                                            graphar::IdType start, graphar::IdType end);

    static void Register(ExtensionLoader& loader);
    static TableFunction GetFunction();
};
}  // namespace duckdb