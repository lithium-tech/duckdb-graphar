#define DUCKDB_EXTENSION_MAIN

#include "graphar_duck_extension.hpp"

#include "functions/table/read_vertices.hpp"
#include "functions/table/read_edges.hpp"
#include "functions/scalar/bfs.hpp"
#include "functions/table/edges_vertex.hpp"
#include "functions/table/hop.hpp"

#include "storage/graphar_storage.hpp"

#include "utils/global_log_manager.hpp"

#include <duckdb.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/extension_util.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

inline void GraphArDuckScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
		    return StringVector::AddString(result, "GraphArDuck "+name.GetString()+" 🐥");
	    });
}

static void LoadInternal(DatabaseInstance &instance) {
	// Register a scalar function
	auto graphar_duck_scalar_function = ScalarFunction("graphar_duck", {LogicalType::VARCHAR}, LogicalType::VARCHAR, GraphArDuckScalarFun);
	ExtensionUtil::RegisterFunction(instance, graphar_duck_scalar_function);

	auto &config = DBConfig::GetConfig(instance);

	config.AddExtensionOption(
	    "graphar_time_logging",
	    "Enable time logging for GraphAr requests.",
	    LogicalType::BOOLEAN,
	    Value::BOOLEAN(false)
	);

	GlobalLogManager::Initialize(instance);

	ReadVertices::Register(instance);
	ReadEdges::Register(instance);
	Bfs::Register(instance);
	EdgesVertex::Register(instance);
	TwoHop::Register(instance);
	OneMoreHop::Register(instance);

	config.storage_extensions["graphar_duck"] = make_uniq<GraphArStorageExtension>();
}

void GrapharDuckExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string GrapharDuckExtension::Name() {
	return "graphar_duck";
}

std::string GrapharDuckExtension::Version() const {
#ifdef EXT_VERSION_GRAPHAR_DUCK
	return EXT_VERSION_GRAPHAR_DUCK;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void graphar_duck_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::GrapharDuckExtension>();
}

DUCKDB_EXTENSION_API const char *graphar_duck_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
