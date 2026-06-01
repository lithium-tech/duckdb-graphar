#pragma once

#include <duckdb/function/table_function.hpp>

#include <atomic>
#include <duckdb.hpp>

namespace duckdb {

class GraphArVersionData : public GlobalTableFunctionState {
public:
    GraphArVersionData() = default;

    std::atomic<bool> finished{false};
};

class GraphArInfo : public TableFunction {
public:
    static unique_ptr<FunctionData> Bind(ClientContext& context, TableFunctionBindInput& input,
                                         vector<LogicalType>& return_types, vector<string>& names);
    static unique_ptr<GlobalTableFunctionState> Init(ClientContext& context, TableFunctionInitInput& input);
    static void Execute(ClientContext& context, TableFunctionInput& input, DataChunk& output);
    static TableFunctionSet GetFunctions();
    static void Register(ExtensionLoader& loader) { loader.RegisterFunction(GetFunctions()); }
};

}  // namespace duckdb
