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

private:
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
    std::shared_ptr<DuckEdgeReader> edge_reader;
    
    friend class ReadHopFiltered;
};

class ReadHopFiltered : public ReadBase<ReadHopFiltered> {
public:
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
