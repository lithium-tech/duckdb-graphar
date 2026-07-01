#pragma once

#include "functions/table/read_base.hpp"

#include <duckdb/function/table_function.hpp>
#include <duckdb/main/client_context.hpp>

#include <graphar/graph_info.h>
#include <graphar/high-level/graph_reader.h>

#include <duckdb.hpp>

namespace duckdb {

struct ShortestPathBindData : public ReadBindData {
    ShortestPathBindData() : ReadBindData() {}
    
    graphar::IdType start_id;
    graphar::IdType end_id;
    std::shared_ptr<graphar::EdgeInfo> edge_info;
    std::shared_ptr<graphar::GraphInfo> graph_info;
    std::shared_ptr<graphar::VertexInfo> vertex_info;
};

struct ShortestPathGlobalState : public GlobalTableFunctionState {
    ShortestPathGlobalState() : GlobalTableFunctionState() {}
    
    std::shared_ptr<graphar::EdgesCollection> forward_edges;
    std::shared_ptr<graphar::EdgesCollection> backward_edges;
    graphar::IdType start_id;
    graphar::IdType end_id;
    bool path_found;
    std::vector<graphar::IdType> path;
    idx_t current_step;
};

class ShortestPath {
public:
    static unique_ptr<FunctionData> Bind(ClientContext& context, TableFunctionBindInput& input,
                                         vector<LogicalType>& return_types, vector<string>& names);
    
    static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext& context,
                                                           TableFunctionInitInput& input);
    
    static void Function(ClientContext& context, TableFunctionInput& data_p, DataChunk& output);
    
    static TableFunction GetFunction();
    
    static void Register(ExtensionLoader& loader);
};

}  // namespace duckdb
