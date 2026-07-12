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
    ReadHopGlobalTableFunctionState() = default;
    ReadHopGlobalTableFunctionState(ReadBaseGlobalTableFunctionState& gstate) : HopBaseGlobalTableFunctionState(gstate) {};    

    size_t MoveBaseReaders(size_t state_ind) {
        DUCKDB_GRAPHAR_LOG_TRACE("ReadHopGlobalTableFunctionState::MoveBaseReaders");

        if (vertex_num == -1) {
            vertex_num = (filter_column == SRC_GID_COLUMN)
                                ? GetCountClass::GetCount(graph_info->GetVertexInfo(edge_info->GetSrcType()),
                                                            graph_info->GetPrefix())
                                : GetCountClass::GetCount(graph_info->GetVertexInfo(edge_info->GetDstType()),
                                                            graph_info->GetPrefix());
        }

        if (cur_idx < state_ind) {
            DUCKDB_GRAPHAR_LOG_WARN("state_index(" + std::to_string(state_ind) + ") > cur_index(" + std::to_string(cur_idx) + "): ");
        }
        if (cur_idx == state_ind && !vertexes.empty()) {
            if (cur_idx + 1 == next_hop_idx) {
                storage_state = false;
                DUCKDB_GRAPHAR_LOG_DEBUG("state_index(" + std::to_string(state_ind) + ") cur_index(" + std::to_string(cur_idx) + ") STORAGE finished");
            }

            const auto prefix = graph_info->GetPrefix();

            auto num_ranges = base_readers[0].size();

            auto edge_info = *std::get_if<std::shared_ptr<graphar::EdgeInfo>>(&type_info);
            if (!edge_info) {
                throw InternalException("Failed to get edge info");
            }

            for (size_t r = 0; r < num_ranges && !vertexes.empty(); ++r, ++cur_idx) {
                if (storage_state && cur_idx + 1 == next_hop_idx) {
                    break; // separate storage and non storage state 
                }

                auto vid = vertexes.front();
                vertexes.pop();

                if (vid < 0 || vid >= vertex_num || vid + 1 <= 0 ||
                    vid + 1 > vertex_num) {
                    throw BinderException("Invalid filter vertex id range: " + std::to_string(vid) + " " + std::to_string(vid + 1));
                }

                for (size_t i = 0; i < base_readers.size(); ++i) {
                    if (global_projected_inds[i].empty()) {
                        continue;
                    }
                    FilterByRangeEdge(base_readers[i][r], {vid, vid + 1}, filter_column, edge_info, prefix);
                }
            }
        }
        return cur_idx;
    }

private:
    std::pair<size_t, size_t> special_dst = {-1, -1};
    int64_t vertex_num = -1;

    bool storage_state = true;

    friend class ReadHop;
};

class ReadHopLocalTableFunctionState : public ReadBaseLocalTableFunctionState {
public:
    ReadHopLocalTableFunctionState() = default;
    ReadHopLocalTableFunctionState(ReadBaseLocalTableFunctionState& lstate) : ReadBaseLocalTableFunctionState(lstate) {};

private:
    bool storage_state = true;
    size_t cur_idx;
    friend class ReadHop;
};

class ReadHop : public ReadBase<ReadHop> {
public:
    static void SetBindData(unique_ptr<ReadHopBindData>& bind_data);
    static unique_ptr<FunctionData> Bind(ClientContext& context, TableFunctionBindInput& input,
                                         vector<LogicalType>& return_types, vector<string>& names);
    static BaseReaderPtr GetBaseReader(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate, idx_t ind,
                                       const std::string& filter_column,
                                       std::shared_ptr<graphar::SharedChunkCounter> counter = nullptr);
    static void SetFilter(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate, idx_t ind,
                          const vector<std::pair<int64_t, int64_t>>& vid_ranges, const std::string& filter_column);
    static ReaderPtr GetReader(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate,
                               ReadBaseLocalTableFunctionState& lstate, idx_t ind, const std::string& filter_column);

    static unique_ptr<BaseStatistics> GetStatistics(ClientContext& context, const FunctionData* bind_data,
                                                    column_t column_index) {
        throw NotImplementedException("GetStatistics is not implemented for ReadHop");
    }

    static void PushdownComplexFilter(ClientContext& context, LogicalGet& get, FunctionData* bind_data,
                                      vector<unique_ptr<Expression>>& filters) {
        throw NotImplementedException("ReadHop::PushdownComplexFilter");
    }

    static TableFunctionSet GetFunctions();
    static TableFunction GetScanFunction();
    static void Execute(ClientContext& context, TableFunctionInput& input, DataChunk& output);
    static unique_ptr<GlobalTableFunctionState> InitWrapper(ClientContext& context, TableFunctionInitInput& input);
    static unique_ptr<LocalTableFunctionState> InitLocalWrapper(ExecutionContext& context, TableFunctionInitInput& input,
                                                                GlobalTableFunctionState* gstate_ptr);

    template <bool notLocked>
    static idx_t FetchRowsNum(ReadHopGlobalTableFunctionState& gstate, ReadHopLocalTableFunctionState& lstate);

    static void SetTableFuncionParams(TableFunction& fun) {
        fun.init_global = InitWrapper;
        fun.init_local = InitLocalWrapper;

        HopBase::SetFunctionParams(fun);

        fun.filter_pushdown = false;
        fun.projection_pushdown = true;
        // fun.statistics = GetStatistics;
        // fun.pushdown_complex_filter = PushdownComplexFilter;
    }
    static void Register(ExtensionLoader& loader) { loader.RegisterFunction(GetFunctions()); }

    static std::string GetFunctionName() {
        return "read_hop";
    }
};
}  // namespace duckdb
