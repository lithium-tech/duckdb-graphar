#define DUCKDB_EXTENSION_MAIN

#include "duckdb_graphar_extension.hpp"

#include "functions/table/degree.hpp"
#include "functions/table/graphar_info.hpp"
#include "functions/table/read_edges.hpp"
#include "functions/table/read_hop.hpp"
#include "functions/table/read_hop_filtered.hpp"
#include "functions/table/read_vertices.hpp"
#include "functions/table/shortest_path.hpp"
#include "functions/table/two_hop.hpp"
#include "storage/graphar_storage.hpp"
#include "utils/func.hpp"
#include "utils/global_log_manager.hpp"
#include "utils/pua_init.hpp"

#include <arrow/filesystem/filesystem.h>

#include <duckdb/common/exception.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/planner/extension_callback.hpp>

#include <duckdb.hpp>

namespace duckdb {

inline void QuackScalarFun(DataChunk& args, ExpressionState& state, Vector& result) {
    auto& name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
        return StringVector::AddString(result, "DuckDB_Graphar " + name.GetString() + " 🐥");
    });
}

static void ConfigureArrowFileSystemCaCertificates() {
    static std::once_flag once;

    std::call_once(once, [] {
        arrow::fs::FileSystemGlobalOptions options;
        for (const char* name : {"AWS_CA_BUNDLE", "SSL_CERT_FILE", "CURL_CA_BUNDLE", "REQUESTS_CA_BUNDLE"}) {
            const char* value = std::getenv(name);
            if (!value || !*value) {
                continue;
            }
            std::error_code ec;
            if (std::filesystem::is_directory(value, ec)) {
                options.tls_ca_dir_path = value;
            } else {
                options.tls_ca_file_path = value;
            }
            break;
        }

        if (options.tls_ca_dir_path.empty()) {
            if (const char* dir = std::getenv("SSL_CERT_DIR"); dir && *dir) {
                std::error_code ec;
                if (std::filesystem::is_directory(dir, ec)) {
                    options.tls_ca_dir_path = dir;
                }
            }
        }

        if (options.tls_ca_file_path.empty() && options.tls_ca_dir_path.empty()) {
            return;
        }

        const auto status = arrow::fs::Initialize(options);
        if (!status.ok()) {
            throw IOException("Failed to configure Arrow TLS: %s", status.ToString());
        }
    });
}

static void LoadInternal(ExtensionLoader& loader) {
    ConfigureArrowFileSystemCaCertificates();

    auto& config = DBConfig::GetConfig(loader.GetDatabaseInstance());

    config.AddExtensionOption("graphar_time_logging", "Enable time logging for GraphAr requests.", LogicalType::BOOLEAN,
                              Value::BOOLEAN(false));

    config.AddExtensionOption("graphar_internal_reader_type",
                              "Internal reader to use for reading graph data files: 'auto' (default, DuckDB for "
                              "parquet, Arrow otherwise), 'duckdb' (always DuckDB, parquet only), 'arrow' (always "
                              "Arrow).",
                              LogicalType::VARCHAR, Value("auto"));
    config.AddExtensionOption("graphar_pua_sink_jsonl_file_path", "Product usage analytics JSONL spool directory.",
                              LogicalType::VARCHAR, Value(""));
    config.AddExtensionOption("graphar_pua_sink_jsonl_file_rotation_size_bytes",
                              "Product usage analytics segment size in bytes.", LogicalType::UBIGINT,
                              Value::UBIGINT(16ULL * 1024ULL * 1024ULL));
    config.AddExtensionOption("graphar_pua_sink_jsonl_file_rotation_interval_seconds",
                              "Product usage analytics segment age in seconds.", LogicalType::UBIGINT,
                              Value::UBIGINT(24ULL * 60ULL * 60ULL));

    // Initialize GlobalLogManager before using any logging macros
    GlobalLogManager::Initialize(loader.GetDatabaseInstance(), duckdb::LogLevel::LOG_WARNING);

    auto duckdb_graphar_scalar_function =
        ScalarFunction("duckdb_graphar", {LogicalType::VARCHAR}, LogicalType::VARCHAR, QuackScalarFun);
    loader.RegisterFunction(duckdb_graphar_scalar_function);

    ReadVertices::Register(loader);
    ReadEdges::Register(loader);
    Degree::Register(loader);
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