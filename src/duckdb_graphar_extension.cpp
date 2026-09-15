#define DUCKDB_EXTENSION_MAIN

#include "duckdb_graphar_extension.hpp"

#include "config/config_readable_iface.h"
#include "functions/table/edges_vertex.hpp"
#include "functions/table/graphar_info.hpp"
#include "functions/table/read_edges.hpp"
#include "functions/table/read_hop.hpp"
#include "functions/table/read_hop_filtered.hpp"
#include "functions/table/read_vertices.hpp"
#include "functions/table/shortest_path.hpp"
#include "functions/table/two_hop.hpp"
#include "storage/graphar_storage.hpp"
#include "usage_analytics/usage_analytics.h"
#include "utils/func.hpp"
#include "utils/global_log_manager.hpp"

#include <duckdb/common/exception.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/planner/extension_callback.hpp>

#include <duckdb.hpp>

namespace duckdb {

namespace {

constexpr const char* kPuaSinkPathOption = "graphar_pua_sink_jsonl_file_path";
constexpr const char* kPuaSinkRotationSizeOption = "graphar_pua_sink_jsonl_file_rotation_size_bytes";
constexpr const char* kPuaSinkRotationIntervalOption = "graphar_pua_sink_jsonl_file_rotation_interval_seconds";

class DuckDBPuaConfig final : public ::config::IConfigReadable {
public:
    explicit DuckDBPuaConfig(const DBConfig& config) {
        auto setting = [&](const char* name) {
            Value value;
            config.TryGetCurrentSetting(name, value);
            return value.ToString();
        };

        ::config::section_type section;
        section.put("path", setting(kPuaSinkPathOption));
        section.put("rotation_size_bytes", setting(kPuaSinkRotationSizeOption));
        section.put("rotation_interval_seconds", setting(kPuaSinkRotationIntervalOption));
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

}  // namespace

inline void QuackScalarFun(DataChunk& args, ExpressionState& state, Vector& result) {
    auto& name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
        return StringVector::AddString(result, "DuckDB_Graphar " + name.GetString() + " 🐥");
    });
}

static void FinalizeS3(DataChunk& args, ExpressionState& state, Vector& result) {
    if (arrow::fs::IsS3Initialized() && !arrow::fs::IsS3Finalized()) {
        graphar::FinalizeS3();
    }
}

static void LoadInternal(ExtensionLoader& loader) {
    auto& config = DBConfig::GetConfig(loader.GetDatabaseInstance());

    config.AddExtensionOption("graphar_time_logging", "Enable time logging for GraphAr requests.", LogicalType::BOOLEAN,
                              Value::BOOLEAN(false));

    config.AddExtensionOption("graphar_internal_reader_type",
                              "Internal reader to use for reading graph data files: 'auto' (default, DuckDB for "
                              "parquet, Arrow otherwise), 'duckdb' (always DuckDB, parquet only), 'arrow' (always "
                              "Arrow).",
                              LogicalType::VARCHAR, Value("auto"));
    config.AddExtensionOption(kPuaSinkPathOption, "Product usage analytics JSONL spool directory.",
                              LogicalType::VARCHAR, Value(""));
    config.AddExtensionOption(kPuaSinkRotationSizeOption, "Product usage analytics segment size in bytes.",
                              LogicalType::UBIGINT, Value::UBIGINT(16ULL * 1024ULL * 1024ULL));
    config.AddExtensionOption(kPuaSinkRotationIntervalOption, "Product usage analytics segment age in seconds.",
                              LogicalType::UBIGINT, Value::UBIGINT(24ULL * 60ULL * 60ULL));

    DuckDBPuaConfig analytics_config(config);
    auto& tracker = analytics::usage_analytics::Tracker::GetInstance();
    tracker.set_module("duckdb_graphar");
    tracker.init(std::cref(analytics_config));

    // Initialize GlobalLogManager before using any logging macros
    GlobalLogManager::Initialize(loader.GetDatabaseInstance(), duckdb::LogLevel::LOG_WARNING);

    auto duckdb_graphar_scalar_function =
        ScalarFunction("duckdb_graphar", {LogicalType::VARCHAR}, LogicalType::VARCHAR, QuackScalarFun);
    loader.RegisterFunction(duckdb_graphar_scalar_function);

    auto finalize_s3_function = ScalarFunction("duckdb_graphar_finalize_s3", {}, LogicalType::VARCHAR, FinalizeS3);
    loader.RegisterFunction(finalize_s3_function);

    ReadVertices::Register(loader);
    ReadEdges::Register(loader);
    EdgesVertex::Register(loader);
    TwoHop::Register(loader);
    ReadHop::Register(loader);
    ReadHopFiltered::Register(loader);
    GraphArInfo::Register(loader);
    ShortestPath::Register(loader);

    StorageExtension::Register(config, "duckdb_graphar", make_shared_ptr<GraphArStorageExtension>());
}

void DuckdbGrapharExtension::Load(ExtensionLoader& loader) { LoadInternal(loader); }

std::string DuckdbGrapharExtension::Name() { return "duckdb_graphar"; }

std::string DuckdbGrapharExtension::Version() const {
#ifdef EXT_VERSION_DUCKDB_GRAPHAR
    return EXT_VERSION_DUCKDB_GRAPHAR;
#else
    return "";
#endif
}

}  // namespace duckdb
extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(duckdb_graphar, loader) { duckdb::LoadInternal(loader); }
}
