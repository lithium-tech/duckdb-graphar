#include "functions/table/hop.hpp"

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
unique_ptr<FunctionData> Hop::Bind(ClientContext& context, TableFunctionBindInput& input,
                                   vector<LogicalType>& return_types, vector<string>& names) {
    DUCKDB_GRAPHAR_LOG_TRACE("Hop::Bind");
    const bool is_catalog_mode = HopBase::IsCatalogMode(input);

    auto bind_data = make_uniq<HopBindData>();

    if (is_catalog_mode) {
        HopBase::SetBindDataByEdgeTable(context, input, *bind_data);
    } else {
        HopBase::SetBindDataByGraphPath(context, input, *bind_data);
        bind_data->graph_info_path = StringValue::Get(input.inputs[0]);
    }

    HopBase::SetBindDataVids(input, *bind_data);

    auto graph_info = bind_data->graph_info;
    auto edge_info = bind_data->edge_info;
    unique_ptr<ReadBindData> base_bind_data = std::move(bind_data);
    ReadBase::SetBindData(graph_info, edge_info, base_bind_data, GetFunctionName(), 0, 1,
                          {SRC_GID_COLUMN, DST_GID_COLUMN});
    bind_data.reset(static_cast<HopBindData*>(base_bind_data.release()));

    names = bind_data->GetFlattenPropNames();
    const auto& fpt = bind_data->GetFlattenPropTypes();
    std::transform(fpt.begin(), fpt.end(), std::back_inserter(return_types),
                   [](const auto& return_type) { return GraphArFunctions::graphArT2duckT(return_type); });

    HopBase::SetBindDataDstIdx(names, *bind_data);

    return std::move(bind_data);
}
//-------------------------------------------------------------------
// PushdownComplexFilter
//-------------------------------------------------------------------
void Hop::PushdownComplexFilter(ClientContext& context, LogicalGet& get, FunctionData* bind_data,
                                vector<unique_ptr<Expression>>& filters) {
    DUCKDB_GRAPHAR_LOG_TRACE("Hop::PushdownComplexFilter");
    if (!bind_data) {
        throw InternalException("Bind data is nullptr");
    }
    auto& hop_bind_data = bind_data->Cast<HopBindData>();
    for (size_t i = 0; i < filters.size(); ++i) {
        if (i) hop_bind_data.query_filter += " AND ";
        hop_bind_data.query_filter += filters[i]->ToString();
    }
    DUCKDB_GRAPHAR_LOG_DEBUG("Hop::filters<" + std::to_string(filters.size()) + ">:" + hop_bind_data.query_filter);

    vector<unique_ptr<Expression>> filters_new;
    filters = std::move(filters_new);
}
//-------------------------------------------------------------------
// WhichFunction
//-------------------------------------------------------------------
string Hop::WhichFunction(HopBindData& bind_data, TableFunctionInitInput& input) {
    DUCKDB_GRAPHAR_LOG_TRACE("Hop::WhichFunction");

    bool need_filters = !bind_data.query_filter.empty();

    if (need_filters) {
        return "read_hop_filtered";
    }

    bool only_src_dst = true;
    const auto& columns = bind_data.GetFlattenPropNames();
    for (auto& column_id : input.column_ids) {
        if (columns[column_id] != SRC_GID_COLUMN && columns[column_id] != DST_GID_COLUMN) {
            only_src_dst = false;
            break;
        }
    }

    if (only_src_dst) {
        return "two_hop";
    }
    return "read_hop";
}
//-------------------------------------------------------------------
// Init
//-------------------------------------------------------------------
unique_ptr<GlobalTableFunctionState> Hop::Init(ClientContext& context, TableFunctionInitInput& input) {
    DUCKDB_GRAPHAR_LOG_TRACE("Hop::Init");

    auto bind_data = input.bind_data->Cast<HopBindData>();

    auto gstate_ptr = make_uniq<HopGlobalTableFunctionState>();
    auto& gstate = *gstate_ptr;

    const auto& columns = bind_data.GetFlattenPropNames();
    for (auto& column_id : input.column_ids) {
        if (gstate.query.empty()) {
            gstate.query += "SELECT " + columns[column_id];
        } else {
            gstate.query += ", " + columns[column_id];
        }
    }
    gstate.query += " FROM ";
    gstate.query += WhichFunction(bind_data, input);
    gstate.query += "(";

    if (bind_data.table_name.empty()) {
        gstate.query += "'";
        gstate.query += bind_data.graph_info_path;
        gstate.query += "', src='";
        gstate.query += bind_data.GetSrcName();
        gstate.query += "', dst='";
        gstate.query += bind_data.GetDstName();
        gstate.query += ", type='";
        gstate.query += bind_data.edge_info->GetEdgeType();
        gstate.query += "'";
    } else {
        gstate.query += "'";
        gstate.query += bind_data.table_name;
        gstate.query += "'";
        if (!bind_data.catalog_name.empty()) {
            gstate.query += ", catalog='";
            gstate.query += bind_data.catalog_name;
            gstate.query += "'";
        }
    }

    gstate.query += ", vids=[";
    for (auto& vid : bind_data.vids) {
        gstate.query += std::to_string(vid) + ",";
    }
    gstate.query.pop_back();
    gstate.query += "]";
    gstate.query += ")";

    if (!bind_data.query_filter.empty()) {
        gstate.query += " WHERE " + bind_data.query_filter;
    }
    gstate.query += ";";

    gstate.conn = make_uniq<duckdb::Connection>(*context.db);
    DUCKDB_GRAPHAR_LOG_WARN("Hop::Query = " + gstate.query);
    gstate.result = gstate.conn->Query(gstate.query);

    return gstate_ptr;
}
//-------------------------------------------------------------------
// Execute
//-------------------------------------------------------------------
void Hop::Execute(ClientContext& context, TableFunctionInput& input, DataChunk& output) {
    DUCKDB_GRAPHAR_LOG_TRACE("Hop::Execute");
    auto& gstate = input.global_state->Cast<HopGlobalTableFunctionState>();
    std::unique_ptr<DataChunk> data = std::move(gstate.result->Fetch());
    if (data) {
        output.Reference(*data);
    } else {
        output.SetCardinality(0);
    }
}
//-------------------------------------------------------------------
// GetFunction
//-------------------------------------------------------------------
TableFunctionSet Hop::GetFunctions() {
    TableFunctionSet hop(GetFunctionName());

    TableFunction hop_default({LogicalType::VARCHAR}, Execute, Bind);
    SetTableFuncionParams(hop_default);
    hop.AddFunction(hop_default);

    return hop;
}
}  // namespace duckdb