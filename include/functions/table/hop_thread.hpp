#pragma once

#include "readers/low_edge_reader.hpp"
#include "utils/func.hpp"

#include <duckdb/common/named_parameter_map.hpp>
#include <duckdb/function/table/arrow.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>

#include <graphar/api/high_level_reader.h>
#include <graphar/graph_info.h>

namespace duckdb {
class TwoHopThreadsBindData final : public TableFunctionData {
public:
    TwoHopThreadsBindData(std::string edge_info_path, graphar::IdType vid)
        : edge_info_path(edge_info_path), vid(vid) {};

public:
    const std::string edge_info_path;
    const graphar::IdType vid;
};

struct TwoHopThreadsGlobalState {
public:
    TwoHopThreadsGlobalState(ClientContext& context, const TwoHopThreadsBindData& bind_data) {};

public:
    std::queue<std::unique_ptr<LowEdgeReaderByVertex>> vertex_readers;
    std::mutex vertex_readers_mutex;
};

struct TwoHopThreadsGlobalTableFunctionState : public GlobalTableFunctionState {
public:
    TwoHopThreadsGlobalTableFunctionState(ClientContext& context, const TwoHopThreadsBindData& bind_data)
        : state(context, bind_data) {};

    static unique_ptr<GlobalTableFunctionState> Init(ClientContext& context, TableFunctionInitInput& input);

    TwoHopThreadsGlobalState& GetState() { return state; }

    idx_t MaxThreads() const override { return GlobalTableFunctionState::MAX_THREADS; }

private:
    TwoHopThreadsGlobalState state;
};

struct TwoHopThreadsLocalTableFunctionState : public LocalTableFunctionState {
public:
    TwoHopThreadsLocalTableFunctionState() {};

    static unique_ptr<LocalTableFunctionState> Init(ExecutionContext& context, TableFunctionInitInput& input,
                                                    GlobalTableFunctionState* global_state);
};

struct TwoHopThreads {
    static unique_ptr<FunctionData> Bind(ClientContext& context, TableFunctionBindInput& input,
                                         vector<LogicalType>& return_types, vector<string>& names);
    static void Execute(ClientContext& context, TableFunctionInput& input, DataChunk& output);
    static TableFunction GetFunction();
    static void Register(ExtensionLoader& loader);
};

}  // namespace duckdb