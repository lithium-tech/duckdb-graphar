#pragma once

#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>

namespace duckdb {
struct Bfs {
    static void Register(ExtensionLoader& loader);
    static ScalarFunction GetFunctionExists();
    static ScalarFunction GetFunctionLength();

    static void WayLength(DataChunk& args, ExpressionState& state, Vector& result);
    static void WayExists(DataChunk& args, ExpressionState& state, Vector& result);
};
}  // namespace duckdb
