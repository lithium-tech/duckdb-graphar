#pragma once

#include <duckdb/common/exception.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/value.hpp>

#include <chrono>
#include <duckdb.hpp>
#include <string>

namespace duckdb {

class ScopedTimer {
    std::chrono::time_point<std::chrono::high_resolution_clock> start, last;
    std::string name;

public:
    explicit ScopedTimer(const std::string& name);
    ~ScopedTimer() {};

    void print(const std::string& msg, bool all = false);
    void print() { print("", true); };
};

struct GraphArSettings {
    template <typename T>
    static T get(const ClientContext& context, const std::string& name) {
        Value result;
        (void)context.TryGetCurrentSetting(name, result);
        if (!result.IsNull()) {
            return !result.IsNull() && result.GetValue<T>();
        }
        return T();
    }

    static bool is_time_logging(const ClientContext& context) { return get<bool>(context, "graphar_time_logging"); }

    // Whether duckdb-graphar should read data files with DuckDB's own parquet
    // reader, or with Arrow's reader. "auto" (default) uses DuckDB for parquet
    // and Arrow for everything else. "duckdb" forces DuckDB (errors on non-parquet).
    // "arrow" forces Arrow (works for all file types).
    static std::string internal_reader_type(const ClientContext& context) {
        Value result;
        (void)context.TryGetCurrentSetting("graphar_internal_reader_type", result);
        if (!result.IsNull()) {
            auto str = result.GetValue<std::string>();
            if (str == "auto" || str == "duckdb" || str == "arrow") {
                return str;
            }
            throw InvalidInputException(
                "graphar_internal_reader_type must be one of: 'auto', 'duckdb', 'arrow'. Got: %s", str);
        }
        return "auto";
    }

    // Decide whether a data file should be read by DuckDB's reader or Arrow's
    // reader, based on the graphar_internal_reader_type setting and the file
    // type. "auto" => DuckDB for parquet, Arrow otherwise. "duckdb" => always
    // DuckDB (throws on non-parquet). "arrow" => always Arrow.
    static bool use_duck_reader(const ClientContext& context, bool is_parquet) {
        const auto mode = internal_reader_type(context);
        if (mode == "duckdb") {
            if (!is_parquet) {
                throw InvalidInputException(
                    "graphar_internal_reader_type='duckdb' is only supported for parquet files, but the target file "
                    "type is not PARQUET. Use 'auto' or 'arrow'.");
            }
            return true;
        }
        if (mode == "arrow") {
            return false;
        }
        return is_parquet;  // 'auto'
    }
};
}  // namespace duckdb
