#pragma once

#include "functions/table/read_base.hpp"
#include "functions/table/hop_base.hpp"

#include <duckdb/common/named_parameter_map.hpp>
#include <duckdb/function/table/arrow/arrow_duck_schema.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>

#include <graphar/api/high_level_reader.h>
#include <graphar/arrow/chunk_reader.h>
#include <graphar/graph_info.h>

#include <cassert>
#include <cxxabi.h>

namespace duckdb {

class ReadHopBindData : public HopBaseBindData {
    friend class ReadHop;
};

class ReadHopGlobalTableFunctionState : public HopBaseGlobalTableFunctionState {
public:
    size_t MoveBaseReaders(size_t state_ind) {
        if (cur_idx < state_ind) {
            DUCKDB_GRAPHAR_LOG_DEBUG("state_index(" + std::to_string(state_ind) + ") > cur_index(" + std::to_string(cur_idx) + "): ");
        }
        if (cur_idx == state_ind && !vertexes.empty()) {
            cur_idx++;

            if (cur_idx == next_hop_idx) {
                storage_state = false;
            }

            const auto prefix = graph_info->GetPrefix();
            DUCKDB_GRAPHAR_LOG_DEBUG("Before move readers");

            auto vid = vertexes.front();
            vertexes.pop();
            for (size_t i = 0; i < base_readers.size(); ++i) {
                if (global_projected_inds[i].empty()) {
                    continue;
                }

                auto& base_reader = base_readers[i];
                FilterByRangeEdge(base_reader, {vid, vid + 1}, SRC_GID_COLUMN, edge_info, prefix);
                PrintFilterInfo(base_reader);
            }
        }
        return cur_idx;
    }

private:
    std::pair<size_t, size_t> special_dst = {-1, -1};

    bool storage_state = true;

    friend class ReadHop;
};

class ReadHopLocalTableFunctionState : public ReadBaseLocalTableFunctionState {
private:
    bool storage_state = true;
    size_t cur_ind;
    friend class ReadHop;
};

class ReadHop : public ReadBase<ReadHop> {
public:
    static void SetBindData(unique_ptr<ReadHopBindData>& bind_data);
    static unique_ptr<FunctionData> Bind(ClientContext& context, TableFunctionBindInput& input,
                                         vector<LogicalType>& return_types, vector<string>& names);
    static BaseReaderPtr GetBaseReader(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate, idx_t ind,
                                       const std::string& filter_column);
    static void SetFilter(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate, idx_t ind,
                          const std::pair<int64_t, int64_t>& vid_range, const std::string& filter_column);
    static ReaderPtr GetReader(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate,
                               ReadBaseLocalTableFunctionState& lstate, idx_t ind, const std::string& filter_column);

    static unique_ptr<BaseStatistics> GetStatistics(ClientContext& context, const FunctionData* bind_data,
                                                    column_t column_index);

    static void PushdownComplexFilter(ClientContext& context, LogicalGet& get, FunctionData* bind_data,
                                      vector<unique_ptr<Expression>>& filters);

    static TableFunctionSet GetFunctions();
    static TableFunction GetScanFunction();
    static void Execute(ClientContext& context, TableFunctionInput& input, DataChunk& output);
    static unique_ptr<GlobalTableFunctionState> Init(ClientContext& context, TableFunctionInitInput& input);
    static unique_ptr<LocalTableFunctionState> InitLocal(ExecutionContext& context, TableFunctionInitInput& input,
                                                         GlobalTableFunctionState* gstate_ptr);
    static void SetTableFuncionParams(TableFunction& fun) {
        fun.init_global = Init;
        fun.init_local = InitLocal;

        HopBase::SetFunctionParams(fun);

        fun.filter_pushdown = false;
        fun.projection_pushdown = true;
        // fun.statistics = GetStatistics;
        fun.pushdown_complex_filter = PushdownComplexFilter;
    }
    static void Register(ExtensionLoader& loader) { loader.RegisterFunction(GetFunctions()); }

    static std::string GetFunctionName() {
        return "read_hop";
    }
};
}  // namespace duckdb
