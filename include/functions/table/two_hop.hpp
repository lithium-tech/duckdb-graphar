#pragma once

#include "readers/low_edge_reader.hpp"
#include "utils/func.hpp"
#include "functions/table/hop_base.hpp"

#include <duckdb/common/named_parameter_map.hpp>
#include <duckdb/function/table/arrow.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>

#include <graphar/api/high_level_reader.h>
#include <graphar/graph_info.h>

namespace duckdb {
class TwoHopBindData final : public HopBaseBindData {
    
    friend class TwoHop;
};

class TwoHopGlobalTableFunctionState : public HopBaseGlobalTableFunctionState {
private:
    std::string edge_info_prefix;
    
    friend class TwoHop;
};

class TwoHopLocalTableFunctionState : public LocalTableFunctionState {
public:
    TwoHopLocalTableFunctionState(const std::shared_ptr<graphar::EdgeInfo> edge_info, const std::string& edge_info_prefix, ExecutionContext& context) {
        DUCKDB_GRAPHAR_LOG_TRACE("TwoHopLocalTableFunctionState");
        reader = make_uniq<LowEdgeReaderByVertex>(edge_info, edge_info_prefix, graphar::AdjListType::ordered_by_source);
        reader->conn = make_uniq<Connection>(*context.client.db);
    }

    void MoveReader(TwoHopGlobalTableFunctionState &gstate) {
        DUCKDB_GRAPHAR_LOG_TRACE("TwoHopLocalTableFunctionState::MoveReader");
        std::lock_guard<std::mutex> lock(gstate.mtx);

        if (!gstate.vertexes.empty()) {
            cur_idx = gstate.cur_idx++;
            reader->SetVertex(gstate.vertexes.front());
            gstate.vertexes.pop();
            in_progress = true;
        } else {
            in_progress = false;
        }
    }

    std::unique_ptr<LowEdgeReaderByVertex> reader;
    bool in_progress = false;
    size_t cur_idx = 0;
};

class TwoHop {
public:
    static unique_ptr<FunctionData> Bind(ClientContext& context, TableFunctionBindInput& input,
                                         vector<LogicalType>& return_types, vector<string>& names);
    static unique_ptr<FunctionData> BindEdgeTable(ClientContext& context, TableFunctionBindInput& input,
                                                  vector<LogicalType>& return_types, vector<string>& names);
    static unique_ptr<FunctionData> BindGraphInfoPath(ClientContext& context, TableFunctionBindInput& input,
                                                      vector<LogicalType>& return_types, vector<string>& names);
    static unique_ptr<FunctionData> BindFinish(ClientContext& context, TableFunctionBindInput& input,
                                               vector<LogicalType>& return_types, vector<string>& names,
                                               unique_ptr<TwoHopBindData> bind_data);
    static unique_ptr<GlobalTableFunctionState> Init(ClientContext& context, TableFunctionInitInput& input);
    static unique_ptr<LocalTableFunctionState> InitLocal(ExecutionContext& context, TableFunctionInitInput& input,
                                                         GlobalTableFunctionState* gstate_ptr);
    static void Execute(ClientContext& context, TableFunctionInput& input, DataChunk& output);

    static TableFunctionSet GetFunctions();
    static void SetTableFuncionParams(TableFunction& fun)  {
        fun.init_global = Init;
        fun.init_local = InitLocal;

        HopBase::SetFunctionParams(fun);
    }
    static void Register(ExtensionLoader& loader) { loader.RegisterFunction(GetFunctions()); }

    static std::string GetFunctionName() {
        return "two_hop";
    }
};

}  // namespace duckdb