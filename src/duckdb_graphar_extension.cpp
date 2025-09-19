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
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include <duckdb.hpp>

namespace duckdb {

inline void QuackScalarFun(DataChunk& args, ExpressionState& state, Vector& result) {
    auto& name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
        return StringVector::AddString(result, "DuckDB_Graphar " + name.GetString() + " 🐥");
    });
}

static void LoadInternal(ExtensionLoader& loader) {
    auto duckdb_graphar_scalar_function =
        ScalarFunction("duckdb_graphar", {LogicalType::VARCHAR}, LogicalType::VARCHAR, QuackScalarFun);
    loader.RegisterFunction(duckdb_graphar_scalar_function);

    auto& config = DBConfig::GetConfig(loader.GetDatabaseInstance());

    config.AddExtensionOption("graphar_time_logging", "Enable time logging for GraphAr requests.", LogicalType::BOOLEAN,
                              Value::BOOLEAN(false));

    GlobalLogManager::Initialize(loader.GetDatabaseInstance());

    ReadVertices::Register(loader);
    ReadEdges::Register(loader);
    Bfs::Register(loader);
    EdgesVertex::Register(loader);
    TwoHop::Register(loader);
    OneMoreHop::Register(loader);

    config.storage_extensions["duckdb_graphar"] = make_uniq<GraphArStorageExtension>();
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