#include "functions/table/graphar_info.hpp"

#include "utils/func.hpp"

#include <cstdint>

namespace duckdb {

unique_ptr<FunctionData> GraphArInfo::Bind(ClientContext& context, TableFunctionBindInput& input,
                                           vector<LogicalType>& return_types, vector<string>& names) {
    DUCKDB_GRAPHAR_LOG_TRACE("GraphArInfo::Bind");
    names.emplace_back("extension_commit");
    return_types.emplace_back(GraphArFunctions::graphArT2duckT("string"));
    names.emplace_back("build_timestamp");
    return_types.emplace_back(GraphArFunctions::graphArT2duckT("timestamp_tz"));
    return nullptr;
}

unique_ptr<GlobalTableFunctionState> GraphArInfo::Init(ClientContext& context, TableFunctionInitInput& input) {
    DUCKDB_GRAPHAR_LOG_TRACE("GraphArInfo::Init");
    return make_uniq<GraphArVersionData>();
}

void GraphArInfo::Execute(ClientContext& context, TableFunctionInput& data_p, DataChunk& output) {
    DUCKDB_GRAPHAR_LOG_TRACE("GraphArInfo::Execute");
    auto& data = data_p.global_state->Cast<GraphArVersionData>();
    if (data.finished.exchange(true, std::memory_order_relaxed)) {
        return;
    }
    output.SetCardinality(1);

#ifdef EXTENSION_GIT_COMMIT_HASH
    output.SetValue(0, 0, Value(EXTENSION_GIT_COMMIT_HASH));
#else
    output.SetValue(0, 0, Value(GraphArFunctions::graphArT2duckT("string")));
#endif

#ifdef EXTENSION_BUILD_TIMESTAMP
    output.SetValue(1, 0, Value(EXTENSION_BUILD_TIMESTAMP));
#else
    output.SetValue(1, 0, Value(GraphArFunctions::graphArT2duckT("timestamp_tz")));
#endif
}

TableFunctionSet GraphArInfo::GetFunctions() {
    TableFunction information_f = TableFunction("graphar_info", {}, Execute, Bind, Init);
    return TableFunctionSet(information_f);
}

}  // namespace duckdb