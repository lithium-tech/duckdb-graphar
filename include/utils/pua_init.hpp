#pragma once

#include "config/config_readable_iface.h"
#include "usage_analytics/usage_analytics.h"

#include <duckdb/main/client_context.hpp>
#include <duckdb/main/config.hpp>

#include <mutex>

namespace duckdb {

// Reads the product-usage-analytics extension options from the DuckDB config and
// exposes them through the generic analytics configuration interface. The values
// are read from the ClientContext so that session-level SET statements (executed
// before the first event) are honored.
class DuckDBPuaConfig final : public ::config::IConfigReadable {
public:
    explicit DuckDBPuaConfig(const ClientContext& context) {
        auto setting = [&](const char* name) {
            Value value;
            context.TryGetCurrentSetting(name, value);
            return value.ToString();
        };

        ::config::section_type section;
        section.put("path", setting("graphar_pua_sink_jsonl_file_path"));
        section.put("rotation_size_bytes", setting("graphar_pua_sink_jsonl_file_rotation_size_bytes"));
        section.put("rotation_interval_seconds", setting("graphar_pua_sink_jsonl_file_rotation_interval_seconds"));
        config_.put_child("pua_sink_jsonl_file", std::move(section));
    }

    const ::config::section_type& GetSection(const std::string& section_name) const override {
        if (section_name.empty()) {
            return config_;
        }
        if (const auto section = config_.get_child_optional(section_name)) {
            return section.get();
        }
        static const ::config::section_type empty_section;
        return empty_section;
    }

    std::string GetParameter(const std::string& section_name, const std::string& parameter_name,
                             const std::string& default_value) const override {
        const std::string path = section_name.empty() ? parameter_name : section_name + "." + parameter_name;
        return config_.get<std::string>(path, default_value);
    }

private:
    ::config::config_tree_type config_;
};

namespace usage_analytics {

inline std::string GetActiveQueryId(ClientContext& context) {
    if (context.transaction.HasActiveTransaction()) {
        return std::to_string(context.transaction.GetActiveQuery());
    }
    return "no_transaction";
}

// Initializes the tracker lazily on first use, reading the current config from
// the ClientContext at that moment. This allows users to SET the analytics
// options before the first event (e.g. before attaching a graph), while changes
// made later in the session are ignored.
inline void EnsureInitialized(ClientContext& context) {
    static std::mutex mutex;
    static bool initialized = false;

    std::lock_guard<std::mutex> lock(mutex);
    if (initialized) {
        return;
    }

    auto& tracker = analytics::usage_analytics::Tracker::GetInstance();
    tracker.set_module("duckdb_graphar");
    DuckDBPuaConfig analytics_config(context);
    tracker.init(std::cref(analytics_config));
    initialized = true;
}

inline void EmitGraphOperationEvent(ClientContext& context, const std::string& function_name,
                                    const std::string& table_name = "") {
    EnsureInitialized(context);
    auto& tracker = analytics::usage_analytics::Tracker::GetInstance();
    const auto process_id = std::string(tracker.common_fields().at("process_id").as_string());
    boost::json::object payload;
    payload["function"] = function_name;
    if (!table_name.empty()) {
        payload["table"] = table_name;
    }
    tracker.emit(analytics::usage_analytics::MakeQuerySession(process_id, GetActiveQueryId(context)),
                 analytics::usage_analytics::EventCode::Event, std::move(payload));
}

}  // namespace usage_analytics

}  // namespace duckdb