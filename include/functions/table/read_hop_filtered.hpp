#pragma once

#include "functions/table/read_base.hpp"
#include "functions/table/hop_base.hpp"
#include "readers/duck_read_edges_reader.hpp"

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

class ReadHopFilteredBindData : public HopBaseBindData {
public:
    std::string query_filter;
    std::string graph_info_path;

    friend class ReadHopFiltered;
};

class ReadHopFilteredGlobalTableFunctionState : public HopBaseGlobalTableFunctionState {
public:
    ReadHopFilteredGlobalTableFunctionState() = default;
    ReadHopFilteredGlobalTableFunctionState(ReadBaseGlobalTableFunctionState& gstate) : HopBaseGlobalTableFunctionState(gstate) {}; 

    size_t MoveBaseReaders(size_t state_ind, bool force = false) {
        DUCKDB_GRAPHAR_LOG_TRACE("ReadHopFilteredGlobalTableFunctionState::MoveBaseReaders");

        if (cur_idx < state_ind) {
            DUCKDB_GRAPHAR_LOG_WARN("state_index(" + std::to_string(state_ind) + ") > cur_index(" + std::to_string(cur_idx) + "): ");
        }

        if (cur_idx == state_ind || force) {
            if (cur_idx == next_hop_idx) {
                storage_state = false;
            }
            const auto prefix = graph_info->GetPrefix();

            auto num_ranges = base_readers[0].size();

            for (size_t r = 0; r < num_ranges && !vertexes.empty(); ++r, ++cur_idx) {
                if (storage_state && cur_idx == next_hop_idx) {
                    break; // separate storage and non storage state 
                }

                auto vid = vertexes.front();
                vertexes.pop();

                DUCKDB_GRAPHAR_LOG_DEBUG("use vid: " + std::to_string(vid) + " query: " + query_string);

                for (size_t i = 0; i < base_readers.size(); ++i) {
                    if (global_projected_inds[i].empty()) {
                        continue;
                    }

                    std::visit(
                        [&](auto& r) {
                            if constexpr (requires { r->callQuery(vid); }) {
                                r->callQuery(vid);
                            } else {
                                throw InternalException("callQuery not implemented for this reader");
                            }
                        },
                    base_readers[i][r]);

                }
            }
        }
        return cur_idx;
    }

    void GenerateQuery(const ReadHopFilteredBindData& bind_data) {
        std::string columns = "";
        for (auto& col_id : column_ids) {
            if (!columns.empty()) {
                columns += ", ";
            }
            columns += "#" + std::to_string(col_id + 1);
        }

        if (!bind_data.graph_info_path.empty()) {
            query_string = "SELECT " + columns + " FROM read_edges('" + graph_info_path + "', src='" +
                                edge_info->GetSrcType() + "', type='" + edge_info->GetEdgeType() + "', dst='" +
                                edge_info->GetDstType() + "') WHERE _graphArSrcIndex = $1";
        } else if (!bind_data.table_name.empty()) {
            query_string = "SELECT " + columns + " FROM " + bind_data.full_table_name() + " WHERE _graphArSrcIndex = $1";
        } else {
            throw InternalException("Either graph_info_path or table_name must be provided");
        }

        if (!query_filter.empty()) {
            query_string += " AND " + query_filter;
        }
    }

private:
    bool storage_state = true;

    std::pair<size_t, size_t> special_dst = {-1, -1};

    std::string query_string;
    std::string query_filter;
    std::string graph_info_path;

    friend class ReadHopFiltered;
};

class ReadHopFilteredLocalTableFunctionState : public ReadBaseLocalTableFunctionState {
public:
    ReadHopFilteredLocalTableFunctionState() = default;
    ReadHopFilteredLocalTableFunctionState(ReadBaseLocalTableFunctionState& lstate) : ReadBaseLocalTableFunctionState(lstate) {};

private:
    bool storage_state = true;
    std::shared_ptr<DuckEdgeReader> edge_reader;
    
    size_t cur_idx;

    friend class ReadHopFiltered;
};

class ReadHopFiltered : public ReadBase<ReadHopFiltered> {
public:
    static void SetBindData(unique_ptr<ReadHopFilteredBindData>& bind_data);
    static unique_ptr<FunctionData> Bind(ClientContext& context, TableFunctionBindInput& input,
                                         vector<LogicalType>& return_types, vector<string>& names);

    static BaseReaderPtr GetBaseReader(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate, idx_t ind,
                                       const std::string& filter_column,
                                       std::shared_ptr<graphar::SharedChunkCounter> counter = nullptr);
    static void SetFilter(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate, idx_t ind,
                          const vector<std::pair<int64_t, int64_t>>& vid_ranges, const std::string& filter_column) {
        throw NotImplementedException("SetFilter is not implemented for ReadHop");
    }
    static ReaderPtr GetReader(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate,
                               ReadBaseLocalTableFunctionState& lstate, idx_t ind, const std::string& filter_column);

    static unique_ptr<BaseStatistics> GetStatistics(ClientContext& context, const FunctionData* bind_data,
                                                    column_t column_index) {
        throw NotImplementedException("GetStatistics is not implemented for ReadHop");
    }
    static void PushdownComplexFilter(ClientContext& context, LogicalGet& get, FunctionData* bind_data,
                                      vector<unique_ptr<Expression>>& filters);

    static TableFunctionSet GetFunctions();
    static TableFunction GetScanFunction();
    static void Execute(ClientContext& context, TableFunctionInput& input, DataChunk& output);
    static unique_ptr<GlobalTableFunctionState> InitWrapper(ClientContext& context, TableFunctionInitInput& input);
    static unique_ptr<LocalTableFunctionState> InitLocal(ExecutionContext& context, TableFunctionInitInput& input,
                                                         GlobalTableFunctionState* gstate_ptr);
    static void SetTableFuncionParams(TableFunction& fun) {
        fun.init_global = InitWrapper;
        fun.init_local = InitLocal;

        HopBase::SetFunctionParams(fun);

        fun.filter_pushdown = false;
        fun.projection_pushdown = true;
        // fun.statistics = GetStatistics;
        fun.pushdown_complex_filter = PushdownComplexFilter;
    }
    static void Register(ExtensionLoader& loader) { loader.RegisterFunction(GetFunctions()); }

    template <bool notLocked>
    static idx_t FetchRowsNum(ReadHopFilteredGlobalTableFunctionState& gstate, ReadHopFilteredLocalTableFunctionState& lstate);

    static std::string GetFunctionName() {
        return "read_hop_filtered";
    }
};
}  // namespace duckdb
