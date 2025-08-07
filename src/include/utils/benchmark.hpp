#pragma once

#include <chrono>
#include <string>

#include "duckdb.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

class ScopedTimer {
    std::chrono::time_point<std::chrono::high_resolution_clock> start, last;
    std::string name;

   public:
    ScopedTimer(const std::string& name);
    ~ScopedTimer() {};
    void print(const std::string msg, bool all);
    void print(const std::string msg) { print(msg, false); };
    void print() { print("", true); };
};

struct GraphArSettings {
    template <typename T>
    static T get(const ClientContext& context, const std::string& name) {
        Value result;
        (void) context.TryGetCurrentSetting(name, result);
        if (!result.IsNull()) {
            return !result.IsNull() && result.GetValue<T>();
        }
        return T();
    }

    static bool is_time_logging(const ClientContext& context) { return get<bool>(context, "graphar_time_logging"); }
};
}  // namespace duckdb
