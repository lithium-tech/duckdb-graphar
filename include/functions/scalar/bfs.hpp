#pragma once

#include "duckdb/function/scalar_function.hpp"

namespace duckdb {
struct Bfs {
    static void Register(DatabaseInstance& db);
    static ScalarFunction GetFunctionExists();
    static ScalarFunction GetFunctionLength();

    static void WayLength(DataChunk& args, ExpressionState& state, Vector& result);
    static void WayExists(DataChunk& args, ExpressionState& state, Vector& result);
};
}  // namespace duckdb
