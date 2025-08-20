#define DUCKDB_EXTENSION_MAIN

#include "duckdb_graphar_extension.hpp"

#include "functions/scalar/bfs.hpp"
#include "functions/table/edges_vertex.hpp"
#include "functions/table/hop.hpp"
#include "functions/table/read_edges.hpp"
#include "functions/table/read_vertices.hpp"
#include "storage/graphar_storage.hpp"
#include "utils/global_log_manager.hpp"

#include <duckdb/common/exception.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/extension_util.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include <duckdb.hpp>

namespace duckdb {

inline void DuckdbGrapharScalarFun(DataChunk& args, ExpressionState& state, Vector& result) {
    auto& name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
        return StringVector::AddString(result, "DuckDB_Graphar " + name.GetString() + " 🐥");
    });
}

static void LoadInternal(DatabaseInstance& instance) {
    // Register a scalar function
    auto duckdb_graphar_scalar_function =
        ScalarFunction("duckdb_graphar", {LogicalType::VARCHAR}, LogicalType::VARCHAR, DuckdbGrapharScalarFun);
    ExtensionUtil::RegisterFunction(instance, duckdb_graphar_scalar_function);

    auto& config = DBConfig::GetConfig(instance);

    config.AddExtensionOption("graphar_time_logging", "Enable time logging for GraphAr requests.", LogicalType::BOOLEAN,
                              Value::BOOLEAN(false));

    GlobalLogManager::Initialize(instance);

    ReadVertices::Register(instance);
    ReadEdges::Register(instance);
    Bfs::Register(instance);
    EdgesVertex::Register(instance);
    TwoHop::Register(instance);
    OneMoreHop::Register(instance);

    config.storage_extensions["duckdb_graphar"] = make_uniq<GraphArStorageExtension>();
}

void DuckdbGrapharExtension::Load(DuckDB& db) { LoadInternal(*db.instance); }
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

DUCKDB_EXTENSION_API void duckdb_graphar_init(duckdb::DatabaseInstance& db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::DuckdbGrapharExtension>();
}

DUCKDB_EXTENSION_API const char* duckdb_graphar_version() { return duckdb::DuckDB::LibraryVersion(); }
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
