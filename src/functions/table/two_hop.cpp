#include "functions/table/two_hop.hpp"

#include "functions/table/hop_base.hpp"
#include "storage/graphar_catalog.hpp"
#include "storage/graphar_schema_entry.hpp"
#include "utils/benchmark.hpp"
#include "utils/func.hpp"
#include "utils/global_log_manager.hpp"

#include <duckdb/common/named_parameter_map.hpp>
#include <duckdb/common/vector_size.hpp>
#include <duckdb/function/table/arrow.hpp>
#include <duckdb/function/table_function.hpp>

#include <graphar/api/high_level_reader.h>
#include <graphar/graph_info.h>

#include <iostream>

namespace duckdb {
//-------------------------------------------------------------------
// Bind
//-------------------------------------------------------------------
unique_ptr<FunctionData> TwoHop::Bind(ClientContext& context, TableFunctionBindInput& input,
                                      vector<LogicalType>& return_types, vector<string>& names) {
    DUCKDB_GRAPHAR_LOG_TRACE("TwoHop::Bind");

    const bool is_catalog_mode = HopBase::IsCatalogMode(input);

    auto bind_data = make_uniq<TwoHopBindData>();

    if (is_catalog_mode) {
        HopBase::SetBindDataByEdgeTable(context, input, *bind_data);
    } else {
        HopBase::SetBindDataByGraphPath(context, input, *bind_data);
    }

    HopBase::SetBindDataVids(input, *bind_data);

    return_types.push_back(LogicalType::BIGINT);
    names.push_back(SRC_GID_COLUMN);
    return_types.push_back(LogicalType::BIGINT);
    names.push_back(DST_GID_COLUMN);
    bind_data->dst_column_idx = 1;

    return std::move(bind_data);
}
//-------------------------------------------------------------------
// Init
//-------------------------------------------------------------------
unique_ptr<GlobalTableFunctionState> TwoHop::Init(ClientContext& context, TableFunctionInitInput& input) {
    DUCKDB_GRAPHAR_LOG_TRACE("TwoHop::Init");

    auto bind_data = input.bind_data->Cast<TwoHopBindData>();

    auto gstate_ptr = make_uniq<TwoHopGlobalTableFunctionState>();
    auto& gstate = *gstate_ptr;

    HopBase::SetGlobalState(bind_data, gstate);

    gstate.edge_info_prefix = bind_data.GetGraphInfo()->GetPrefix();

    return gstate_ptr;
}
//-------------------------------------------------------------------
// InitLocal
//-------------------------------------------------------------------
unique_ptr<LocalTableFunctionState> TwoHop::InitLocal(ExecutionContext& context, TableFunctionInitInput& input,
                                                      GlobalTableFunctionState* gstate_ptr) {
    DUCKDB_GRAPHAR_LOG_TRACE("TwoHop::LocalStateInit");

    TwoHopGlobalTableFunctionState& gstate = gstate_ptr->Cast<TwoHopGlobalTableFunctionState>();

    auto lstate_ptr = make_uniq<TwoHopLocalTableFunctionState>(gstate.edge_info, gstate.edge_info_prefix, context);
    auto& lstate = *lstate_ptr;

    lstate.MoveReader(gstate);

    return lstate_ptr;
}
//-------------------------------------------------------------------
// Execute
//-------------------------------------------------------------------
void TwoHop::Execute(ClientContext& context, TableFunctionInput& input, DataChunk& output) {
    DUCKDB_GRAPHAR_LOG_TRACE("TwoHop::Execute")
    auto& gstate = input.global_state->Cast<TwoHopGlobalTableFunctionState>();
    auto& lstate = input.local_state->Cast<TwoHopLocalTableFunctionState>();

    while (lstate.in_progress) {
        std::unique_ptr<DataChunk> data = std::move(lstate.reader->read());
        if (data != nullptr) {
            output.Reference(*data);
            if (lstate.cur_idx < gstate.next_hop_idx) {
                for (idx_t i = 0; i < data->size(); ++i) {
                    auto dst = data->GetValue(gstate.dst_column_idx, i).GetValue<int64_t>();
                    if (!gstate._vertexes.contains(dst)) {
                        gstate.vertexes.push(dst);
                        gstate._vertexes.insert(dst);
                    }
                }
            }
            return;
        }
        DUCKDB_GRAPHAR_LOG_DEBUG("Vertex finished " + std::to_string(lstate.reader->GetVertex()));
        lstate.MoveReader(gstate);
    }
    output.SetCardinality(0);
    DUCKDB_GRAPHAR_LOG_DEBUG("Empty Execute");
}
//-------------------------------------------------------------------
// GetFunction
//-------------------------------------------------------------------
TableFunctionSet TwoHop::GetFunctions() {
    TableFunctionSet two_hop(GetFunctionName());

    TableFunction two_hop_default({LogicalType::VARCHAR}, Execute, Bind);
    SetTableFuncionParams(two_hop_default);
    two_hop.AddFunction(two_hop_default);

    return two_hop;
}
}  // namespace duckdb