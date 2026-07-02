
#include "functions/table/read_edges.hpp"
#include "functions/table/read_hop_filtered.hpp"
#include "utils/benchmark.hpp"
#include "utils/func.hpp"
#include "storage/graphar_catalog.hpp"
#include "storage/graphar_schema_entry.hpp"

#include <arrow/c/bridge.h>

#include <duckdb/common/named_parameter_map.hpp>
#include <duckdb/function/table/arrow.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>

#include <graphar/api/arrow_reader.h>
#include <graphar/api/high_level_reader.h>
#include <graphar/arrow/chunk_reader.h>
#include <graphar/expression.h>
#include <graphar/fwd.h>

#include <iostream>

namespace duckdb {
//-------------------------------------------------------------------
// GetBindData
//-------------------------------------------------------------------
void ReadHopFiltered::SetBindData(unique_ptr<ReadHopFilteredBindData>& bind_data) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHopFiltered::SetBindData")

    ReadBase::SetBindData(bind_data->graph_info, bind_data->edge_info, reinterpret_cast<unique_ptr<ReadBindData>&>(bind_data), GetFunctionName(), 0, 1, {SRC_GID_COLUMN, DST_GID_COLUMN});
}
//-------------------------------------------------------------------
// Bind
//-------------------------------------------------------------------
unique_ptr<FunctionData> ReadHopFiltered::Bind(ClientContext& context, TableFunctionBindInput& input,
                                               vector<LogicalType>& return_types, vector<string>& names) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHopFiltered::Bind")
    const bool is_catalog_mode = HopBase::IsCatalogMode(input);

    auto bind_data = make_uniq<ReadHopFilteredBindData>();

    if (is_catalog_mode) {
        HopBase::SetBindDataByEdgeTable(context, input, return_types, names, *bind_data);
    } else {
        HopBase::SetBindDataByGraphPath(context, input, return_types, names, *bind_data);
    }

    HopBase::SetBindDataVids(input, *bind_data);

    SetBindData(bind_data);

    names = bind_data->flatten_prop_names;
    for (size_t i = 0; i < names.size(); ++i) {
        if (names[i] == DST_GID_COLUMN) {
            bind_data->dst_column_idx = i;
            break;
        }
    }

    std::transform(bind_data->flatten_prop_types.begin(), bind_data->flatten_prop_types.end(),
                   std::back_inserter(return_types),
                   [](const auto& return_type) { return GraphArFunctions::graphArT2duckT(return_type); });

    return std::move(bind_data);
}
//-------------------------------------------------------------------
// GetBaseReader
//-------------------------------------------------------------------
BaseReaderPtr ReadHopFiltered::GetBaseReader(ClientContext& context, ReadHopFilteredGlobalTableFunctionState& gstate, idx_t ind,
                                             const std::string& filter_column) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHopFiltered::GetBaseReader");

    auto conn = std::make_shared<Connection>(*context.db);
    auto query_base_reader = QueryChunkReader::Make(std::move(conn), gstate.query_string);
    BaseReaderPtr base_reader = ConvertBaseReader(query_base_reader);

    return base_reader;
}
//-------------------------------------------------------------------
// SetFilter
//-------------------------------------------------------------------
void ReadHopFiltered::SetFilter(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate, idx_t ind,
                                const std::pair<int64_t, int64_t>& vid_range, const std::string& filter_column) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHopFiltered::SetFilter");
    throw NotImplementedException("SetFilter not implemented for ReadHopFiltered");
}
//-------------------------------------------------------------------
// GetReader
//-------------------------------------------------------------------
ReaderPtr ReadHopFiltered::GetReader(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate,
                             ReadBaseLocalTableFunctionState& lstate, idx_t ind, const std::string& filter_column) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHopFiltered::GetReader");
    auto base_reader = std::get<std::shared_ptr<graphar::TSQueryChunkReader>>(gstate.base_readers[ind]);
    return ConvertReader(graphar::DuckQueryChunkReader::Make(context, base_reader));
}
//-------------------------------------------------------------------
// GetStatistics
//-------------------------------------------------------------------
unique_ptr<BaseStatistics> ReadHopFiltered::GetStatistics(ClientContext& context, const FunctionData* bind_data,
                                                  column_t column_index) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHopFiltered::GetStatistics");
    auto read_bind_data = bind_data->Cast<ReadBindData>();
    if (column_index < 0 || column_index >= read_bind_data.GetFlattenPropTypes().size()) {
        return nullptr;
    }
    auto duck_type = GraphArFunctions::graphArT2duckT(read_bind_data.GetFlattenPropTypes()[column_index]);
    auto column_name = read_bind_data.GetFlattenPropNames()[column_index];
    if (column_name != SRC_GID_COLUMN && column_name != DST_GID_COLUMN) {
        auto stats = BaseStatistics::CreateUnknown(duck_type);
        return stats.ToUnique();
    }

    auto edge_info = *std::get_if<std::shared_ptr<graphar::EdgeInfo>>(&read_bind_data.type_info);
    if (!edge_info) {
        throw InternalException("Failed to get edge info");
    }
    const auto& prefix = read_bind_data.graph_info->GetPrefix();
    auto vertex_num_file_suffix = edge_info->GetVerticesNumFilePath(graphar::AdjListType::ordered_by_source).value();
    const std::string vertex_num_file_path = prefix + vertex_num_file_suffix;

    auto vertex_num = GetCountClass::GetCount(edge_info->GetSrcType(), vertex_num_file_path);

    auto v_type = GetVertexTypeName(read_bind_data.type_info, column_name);
    auto stats = NumericStats::CreateEmpty(LogicalType::BIGINT);
    NumericStats::SetMin(stats, Value::BIGINT(0));
    // TODO: Add get count for vertex by edge info
    NumericStats::SetMax(stats, Value::BIGINT(vertex_num - 1));
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHop::GetStatistics: finished");
    return stats.ToUnique();
}
//-------------------------------------------------------------------
// PushdownComplexFilter
//-------------------------------------------------------------------
void ReadHopFiltered::PushdownComplexFilter(ClientContext& context, LogicalGet& get, FunctionData* bind_data,
                                            vector<unique_ptr<Expression>>& filters) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHopFiltered::PushdownComplexFilter");
    if (!bind_data) {
        throw InternalException("Bind data is nullptr");
    }
    auto read_bind_data = dynamic_cast<ReadHopFilteredBindData*>(bind_data);
    std::string filt;
    for (auto& filter : filters) {
        filt += filter->ToString();
    }
    read_bind_data->query_filter = filt;
    DUCKDB_GRAPHAR_LOG_DEBUG("filters<" + std::to_string(filters.size()) + ">:" + filt);
    
    vector<unique_ptr<Expression>> filters_new;
    filters = std::move(filters_new);
}
//-------------------------------------------------------------------
// GetFunction
//-------------------------------------------------------------------
TableFunctionSet ReadHopFiltered::GetFunctions() {
    TableFunctionSet read_hop_filtered(GetFunctionName());

    TableFunction read_hop_defalt({LogicalType::VARCHAR}, Execute, Bind);

    SetTableFuncionParams(read_hop_defalt);
    read_hop_filtered.AddFunction(read_hop_defalt);
   
    return read_hop_filtered;
}
//-------------------------------------------------------------------
// GetScanFunction
//-------------------------------------------------------------------
TableFunction ReadHopFiltered::GetScanFunction() {
    TableFunction read_hop(GetFunctionName(), {}, Execute, Bind);
    SetTableFuncionParams(read_hop);

    return read_hop;
}
//-------------------------------------------------------------------
// Init
//-------------------------------------------------------------------
unique_ptr<GlobalTableFunctionState> ReadHopFiltered::Init(ClientContext& context, TableFunctionInitInput& input) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHopFiltered::Init");

    auto bind_data = input.bind_data->Cast<ReadHopFilteredBindData>();

    auto gstate_ptr = make_uniq<ReadHopFilteredGlobalTableFunctionState>();
    auto& gstate = *gstate_ptr;

    DUCKDB_GRAPHAR_LOG_DEBUG("Init global state");

    HopBase::SetGlobalState(bind_data, gstate);

    gstate.function_name = bind_data.function_name;
    gstate.id_columns_num = bind_data.id_columns_num;
    gstate.pgs = bind_data.pgs;
    gstate.column_ids = input.column_ids;
    gstate.filter_column = bind_data.filter_column;
    gstate.type_info = bind_data.type_info;
    gstate.graph_info = bind_data.graph_info;
    gstate.params = bind_data.params;
    gstate.query_filter = bind_data.query_filter;
    gstate.graph_info_path = bind_data.graph_info_path;

    gstate.dst_column_found = false;
    for (size_t i = 0; i < gstate.column_ids.size(); i++) {
        if (bind_data.flatten_prop_names[gstate.column_ids[i]] == DST_GID_COLUMN) {
            gstate.dst_column_idx = i;
            gstate.dst_column_found = true;
            break;
        }
    }

    if (!gstate.dst_column_found) {
        DUCKDB_GRAPHAR_LOG_DEBUG("Gstate dst col NOT found")
        for (size_t i = 0; i < bind_data.flatten_prop_names.size(); i++) {
            if (bind_data.flatten_prop_names[i] == DST_GID_COLUMN) {
                gstate.dst_column_idx = gstate.column_ids.size();
                gstate.dst_column_found = true;
                gstate.column_ids.push_back(i);
                break;
            }
        }
        if (!gstate.dst_column_found) {
            throw IOException("Failed to find dst (" + DST_GID_COLUMN + ") column in flatten names");
        }
        gstate.dst_column_found = false;
    }

    gstate.GenerateQuery(bind_data);
    HopBase::SetGlobalState(bind_data, gstate);

    const auto prop_types_size = bind_data.prop_types.size();
    vector<idx_t> columns_pref_num(prop_types_size + 1);
    columns_pref_num[0] = 0;
    for (idx_t i = 0; i < prop_types_size; i++) {
        columns_pref_num[i + 1] = columns_pref_num[i] + bind_data.prop_types[i].size();
    }

    const auto& filter_column = gstate.filter_column;

    gstate.prop_names = std::move(bind_data.prop_names);
    gstate.prop_types = std::move(bind_data.prop_types);
    vector<vector<column_t>> local_projected_inds(prop_types_size);
    gstate.global_projected_inds.resize(prop_types_size);
    gstate.base_readers.resize(prop_types_size);
    if (gstate.column_ids.empty() ||
        gstate.column_ids.size() == 1 && gstate.column_ids[0] == COLUMN_IDENTIFIER_ROW_ID) {
        DUCKDB_GRAPHAR_LOG_WARN("Returning any column");
        local_projected_inds[0].emplace_back(0);
        gstate.base_readers[0] = GetBaseReader(context, gstate, 0, filter_column);
        gstate.global_projected_inds[0].emplace_back(0);
    } else {
        DUCKDB_GRAPHAR_LOG_DEBUG("Base reader size " + std::to_string(gstate.base_readers.size()))
        for (idx_t column_i = 0; column_i < gstate.column_ids.size(); ++column_i) {
            const auto& column_id = gstate.column_ids[column_i];
            const auto i = std::upper_bound(columns_pref_num.begin(), columns_pref_num.end(), column_id) -
                           columns_pref_num.begin() - 1;
            auto projected_ind = column_id - columns_pref_num[i];
            if (!bind_data.pg_for_id && i > 0) {
                projected_ind += bind_data.id_columns_num;
            }
            local_projected_inds[i].emplace_back(projected_ind);
            gstate.global_projected_inds[i].emplace_back(column_i);
        }

        for (idx_t i = 0; i < prop_types_size; ++i) {
            if (local_projected_inds[i].empty()) {
                continue;
            }

            gstate.base_readers[i] = std::move(GetBaseReader(context, gstate, i, filter_column));
        }
    }
    gstate.MoveBaseReaders(0, true);

    DUCKDB_GRAPHAR_LOG_DEBUG("ReadHopFiltered::Init global_projected_inds");
    DUCKDB_GRAPHAR_LOG_DEBUG("readers num: " + std::to_string(gstate.base_readers.size()));

    gstate.local_projected_inds = std::move(local_projected_inds);

    DUCKDB_GRAPHAR_LOG_DEBUG("::Init Done");
    return gstate_ptr;
}
//-------------------------------------------------------------------
// InitLocal
//-------------------------------------------------------------------
unique_ptr<LocalTableFunctionState> ReadHopFiltered::InitLocal(ExecutionContext& context, TableFunctionInitInput& input,
                                                       GlobalTableFunctionState* gstate_ptr) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHop::InitLocal");
    auto bind_data = input.bind_data->Cast<ReadBindData>();

    auto lstate_ptr = make_uniq<ReadHopFilteredLocalTableFunctionState>();
    auto& lstate = *lstate_ptr;
    auto& gstate = gstate_ptr->Cast<ReadHopFilteredGlobalTableFunctionState>();

    lstate.cur_idx = gstate.cur_idx;
    const auto prop_types_size = gstate.prop_types.size();
    lstate.cur_chunks.resize(prop_types_size);
    lstate.readers.resize(prop_types_size);

    for (idx_t i = 0; i < prop_types_size; ++i) {
        if (gstate.local_projected_inds[i].empty()) {
            continue;
        }
        lstate.readers[i] = std::move(GetReader(context.client, gstate, lstate, i, gstate.filter_column));
    }

    lstate.storage_state = gstate.storage_state;

    return lstate_ptr;
}
//-------------------------------------------------------------------
// Execute
//-------------------------------------------------------------------
void ReadHopFiltered::Execute(ClientContext& context, TableFunctionInput& input, DataChunk& output) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHopFiltered::Execute");
    bool time_logging = GraphArSettings::is_time_logging(context);

    ReadHopFilteredGlobalTableFunctionState& gstate = input.global_state->Cast<ReadHopFilteredGlobalTableFunctionState>();
    ReadHopFilteredLocalTableFunctionState& lstate = input.local_state->Cast<ReadHopFilteredLocalTableFunctionState>();

    DUCKDB_GRAPHAR_LOG_DEBUG("Chunk " + std::to_string(gstate.chunk_count) + ": Begin iteration");

    idx_t num_rows = STANDARD_VECTOR_SIZE;

    bool no_more_rows = std::visit([&](auto&& r) -> bool { return r->NoMoreRows(); }, lstate.readers[0]);
    DUCKDB_GRAPHAR_LOG_DEBUG("no more rows: " + std::to_string(no_more_rows));
    if (no_more_rows) {
        std::lock_guard<std::mutex> lock(gstate.mtx);
        for (auto& reader : lstate.readers) {
            if (IsNullPtr(reader) || !num_rows) {
                continue;
            }
            idx_t reserve_rows = ReserveRowsToRead(reader);
            num_rows = std::min(num_rows, reserve_rows);
        }
        lstate.storage_state = gstate.storage_state;
    } else {
        for (auto& reader : lstate.readers) {
            if (IsNullPtr(reader) || !num_rows) {
                continue;
            }
            idx_t reserve_rows = ReserveRowsToRead(reader);
            num_rows = std::min(num_rows, reserve_rows);
        } 
    }
    DUCKDB_GRAPHAR_LOG_DEBUG("num rows pred: " + std::to_string(num_rows));

    if (num_rows == 0) {
        std::lock_guard<std::mutex> lock(gstate.mtx);
        while (!gstate.vertexes.empty() && num_rows == 0) {
            lstate.cur_idx = gstate.MoveBaseReaders(lstate.cur_idx);
            
            num_rows = STANDARD_VECTOR_SIZE;
            for (auto& reader : lstate.readers) {
                if (IsNullPtr(reader) || !num_rows) {
                    continue;
                }
                
                idx_t reserve_rows = ReserveRowsToRead(reader);
                DUCKDB_GRAPHAR_LOG_DEBUG("num rows reserved: " + std::to_string(reserve_rows));
                num_rows = std::min(num_rows, reserve_rows);
            }
            DUCKDB_GRAPHAR_LOG_DEBUG("num rows: " + std::to_string(num_rows));
        }
        lstate.storage_state = gstate.storage_state;
    }

    DUCKDB_GRAPHAR_LOG_DEBUG("num rows final: " + std::to_string(num_rows));

    if (num_rows > 0) {
        for (idx_t i = 0; i < lstate.readers.size(); ++i) {
            if (IsNullPtr(lstate.readers[i])) {
                continue;
            }
            lstate.cur_chunks[i] = std::move(GetChunk(lstate.readers[i], num_rows));
            if (gstate.dst_column_found) {
                output.Reference(*lstate.cur_chunks[i]);
            } else {
                for (idx_t j = 0; j + 1 < lstate.cur_chunks[i]->ColumnCount(); ++j) {
                    output.data[j].Reference(lstate.cur_chunks[i]->data[j]);
                }
            }
        }
        if (lstate.storage_state) {
            for (idx_t i = 0; i < num_rows; i++) {
                graphar::IdType v; 
                if (gstate.dst_column_found) {
                    v = output.data[gstate.dst_column_idx].GetValue(i).GetValue<graphar::IdType>();
                } else {
                    v = lstate.cur_chunks[0]->data[gstate.dst_column_idx].GetValue(i).GetValue<graphar::IdType>();
                }
                // Need check uniq vertexes for 2 hop roots
                if (!gstate._vertexes.contains(v)) {
                    // Need use iters with '<' operator for no double move of base_reader
                    gstate.vertexes.push(v);
                    gstate._vertexes.insert(v);
                }
            }
        }
    }

    output.SetCapacity(num_rows);
    output.SetCardinality(num_rows);
    gstate.total_rows += num_rows;
    DUCKDB_GRAPHAR_LOG_DEBUG("Size of chunk: " + std::to_string(num_rows) +
                             " Total size: " + std::to_string(gstate.total_rows))

    gstate.chunk_count++;

    if (num_rows == 0) {
        DUCKDB_GRAPHAR_LOG_DEBUG("One-hop unique size: " + std::to_string(gstate.next_hop_idx));
    }
}

}  // namespace duckdb
