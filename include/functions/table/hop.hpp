#pragma once

#include "functions/table/hop_base.hpp"
#include "functions/table/read_base.hpp"

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

class HopBindData : public HopBaseBindData {
    std::string query_filter;
    std::string graph_info_path;

    friend class Hop;
};

class HopGlobalTableFunctionState : public GlobalTableFunctionState {
private:
    std::string query;
    std::unique_ptr<duckdb::Connection> conn;
    unique_ptr<QueryResult> result;

    friend class Hop;
};

class Hop : public ReadBase<ReadHop> {
public:
    static unique_ptr<FunctionData> Bind(ClientContext& context, TableFunctionBindInput& input,
                                         vector<LogicalType>& return_types, vector<string>& names);
    static unique_ptr<GlobalTableFunctionState> Init(ClientContext& context, TableFunctionInitInput& input);
    static void Execute(ClientContext& context, TableFunctionInput& input, DataChunk& output);
    static void PushdownComplexFilter(ClientContext& context, LogicalGet& get, FunctionData* bind_data,
                                      vector<unique_ptr<Expression>>& filters);

    static string WhichFunction(HopBindData& bind_data, TableFunctionInitInput& input);

    static TableFunctionSet GetFunctions();
    static void SetTableFuncionParams(TableFunction& fun) {
        fun.init_global = Init;

        HopBase::SetFunctionParams(fun);

        fun.filter_pushdown = false;
        fun.projection_pushdown = true;
        fun.pushdown_complex_filter = PushdownComplexFilter;
    }
    static void Register(ExtensionLoader& loader) { loader.RegisterFunction(GetFunctions()); }

    static std::string GetFunctionName() { return "hop2"; }
};
}  // namespace duckdb
