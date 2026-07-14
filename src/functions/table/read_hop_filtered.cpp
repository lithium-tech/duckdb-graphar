
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
        bind_data->graph_info_path = StringValue::Get(input.inputs[0]);
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
BaseReaderPtr ReadHopFiltered::GetBaseReader(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate, idx_t ind,
                                             const std::string& filter_column,
                                             std::shared_ptr<graphar::SharedChunkCounter> counter) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHopFiltered::GetBaseReader");
    auto& gstate_hop = gstate.Cast<ReadHopFilteredGlobalTableFunctionState>();

    auto vids_reader = graphar::VidsChunkReader::Make();
    // auto conn = std::make_shared<Connection>(*context.db);
    // auto query_base_reader = QueryChunkReader::Make(std::move(conn), gstate_hop.query_string);

    return ConvertBaseReader(vids_reader, counter);
}
//-------------------------------------------------------------------
// GetReader
//-------------------------------------------------------------------
ReaderPtr ReadHopFiltered::GetReader(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate,
                             ReadBaseLocalTableFunctionState& lstate, idx_t ind, const std::string& filter_column) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHopFiltered::GetReader");
    auto& lstate_hop = lstate.Cast<ReadHopFilteredLocalTableFunctionState>();
    
    // auto base_reader = std::get<std::shared_ptr<graphar::TSQueryChunkReader>>(gstate.base_readers[ind][0]);
    // return ConvertReader(graphar::DuckQueryChunkReader::Make(context, base_reader));
    std::vector<std::shared_ptr<graphar::TSVidsChunkReader>> base_readers;
    base_readers.reserve(gstate.base_readers[ind].size());
    for (const auto& base_reader : gstate.base_readers[ind]) {
        base_readers.push_back(std::get<std::shared_ptr<graphar::TSVidsChunkReader>>(base_reader));
    }

    return ConvertReader(graphar::DuckReadEdgesChunkReader::Make(context, lstate_hop.edge_reader, base_readers));
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
    auto& read_bind_data = bind_data->Cast<ReadHopFilteredBindData>();
    std::string filt;
    for (auto& filter : filters) {
        filt += filter->ToString();
    }
    read_bind_data.query_filter = filt;
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
unique_ptr<GlobalTableFunctionState> ReadHopFiltered::InitWrapper(ClientContext& context, TableFunctionInitInput& input) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHopFiltered::Init");

    auto bind_data = input.bind_data->Cast<ReadHopFilteredBindData>();
    bool dst_column_found = true;

    if (std::find(input.column_ids.begin(), input.column_ids.end(), bind_data.dst_column_idx) == input.column_ids.end()) {
        dst_column_found = false;
        input.column_ids.push_back(bind_data.dst_column_idx);
    }

    auto base_gstate_ptr = Init(context, input);
    auto& base_gstate = base_gstate_ptr->Cast<ReadBaseGlobalTableFunctionState>();
    auto gstate_ptr = std::make_unique<ReadHopFilteredGlobalTableFunctionState>(base_gstate);
    auto& gstate = *gstate_ptr;
    gstate.dst_column_found = dst_column_found;

    HopBase::SetGlobalState(bind_data, gstate);

    gstate.query_filter = bind_data.query_filter;
    gstate.graph_info_path = bind_data.graph_info_path;

    auto column_it = std::find(gstate.column_ids.begin(), gstate.column_ids.end(), gstate.dst_column_idx);
    if (column_it == gstate.column_ids.end()) {
        throw InternalException("dst_column_idx(" + std::to_string(gstate.dst_column_idx) + ") not found in column_ids");
    }

    auto column_i = std::distance(gstate.column_ids.begin(), column_it);

    auto columns_pref_num = 0;
    for (auto pg_i = 0; pg_i < gstate.prop_types.size(); columns_pref_num += gstate.prop_types[pg_i].size(), ++pg_i) {
        if (columns_pref_num > gstate.dst_column_idx || gstate.dst_column_idx >= columns_pref_num + gstate.prop_types[pg_i].size()) {
            continue;
        }
        
        auto projected_ind = gstate.dst_column_idx - columns_pref_num;
        if (!bind_data.pg_for_id && pg_i > 0) {
            projected_ind += bind_data.id_columns_num;
        }

        auto global_projected_i = std::find(gstate.global_projected_inds[pg_i].begin(), gstate.global_projected_inds[pg_i].end(), column_i);
        if (global_projected_i == gstate.global_projected_inds[pg_i].end()) {
            throw InternalException("Column DST " + std::to_string(gstate.dst_column_idx) + " not found in the global projected inds");
        }
        gstate.special_dst = {pg_i, global_projected_i - gstate.global_projected_inds[pg_i].begin()}; 
    }

    for (auto &base_readers : gstate.base_readers) {
        for (auto &base_reader : base_readers) {
            std::visit(
                [&gstate_ptr](auto& r) {
                    if constexpr (requires { r->Init(gstate_ptr.get()); }) {
                        r->Init(gstate_ptr.get());
                    } else {
                        throw InternalException("Init not implemented for this reader " + DemangleTypeName(typeid(r).name()));
                    }
                },
            base_reader);
        }
    }

    return gstate_ptr;
}
//-------------------------------------------------------------------
// InitLocal
//-------------------------------------------------------------------
unique_ptr<LocalTableFunctionState> ReadHopFiltered::InitLocal(ExecutionContext& context, TableFunctionInitInput& input,
                                                               GlobalTableFunctionState* gstate_ptr) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHopFiltered::InitLocal");
    auto bind_data = input.bind_data->Cast<ReadHopFilteredBindData>();

    auto lstate_ptr = make_uniq<ReadHopFilteredLocalTableFunctionState>();
    auto& lstate = *lstate_ptr;
    auto& gstate = gstate_ptr->Cast<ReadHopFilteredGlobalTableFunctionState>();

    const auto prop_types_size = gstate.prop_types.size();
    lstate.cur_chunks.resize(prop_types_size);
    lstate.readers.resize(prop_types_size);

    auto conn = std::make_shared<Connection>(*context.client.db);
    auto edge_reader = DuckEdgeReader::Make(conn, bind_data.full_table_name(), gstate.graph_info_path, gstate.edge_info);
    if (edge_reader.has_error()) {
        throw InternalException("Failed to create edge reader: |" + edge_reader.status().message() + "|");
    }
    
    lstate.edge_reader = edge_reader.value();

    for (idx_t i = 0; i < prop_types_size; ++i) {
        if (gstate.global_projected_inds[i].empty()) {
            continue;
        }
        lstate.readers[i] = std::move(GetReader(context.client, gstate, lstate, i, gstate.filter_column));
        SelectColumns(lstate.readers[i], gstate.global_projected_inds[i]);
    }

    return lstate_ptr;
}
//-------------------------------------------------------------------
// Execute
//-------------------------------------------------------------------
template <bool notLocked>
idx_t ReadHopFiltered::FetchRowsNum(ReadHopFilteredGlobalTableFunctionState& gstate, ReadHopFilteredLocalTableFunctionState& lstate) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHopFiltered::FetchRowsNum");

    bool found_reader = false;
    bool needs_new_file = false;
    for (auto& reader : lstate.readers) {
        if (IsNullPtr(reader)) continue;
        if (found_reader) {
            if (CheckIfNewFileNeeded(reader) != needs_new_file) {
                throw InternalException("All readers should have the same needs_new_file status");
            }
        } else {
            found_reader = true;
            needs_new_file = CheckIfNewFileNeeded(reader);
        }
    }

    if (needs_new_file) {
        size_t first_reader;
        found_reader = false;
        if constexpr (notLocked) {
            std::lock_guard<std::mutex> guard(gstate.lock);

            for (size_t i = 0; i < lstate.readers.size(); ++i) {
                if (IsNullPtr(lstate.readers[i])) continue;
                if (!found_reader) {
                    first_reader = i;
                    found_reader = true;
                    AcquirePathUnderLock(lstate.readers[i]);
                } else {
                    CopyVidFrom(lstate.readers[first_reader], lstate.readers[i]);
                }
            }
        } else {
            for (size_t i = 0; i < lstate.readers.size(); ++i) {
                if (IsNullPtr(lstate.readers[i])) continue;
                if (!found_reader) {
                    first_reader = i;
                    found_reader = true;
                    AcquirePathUnderLock(lstate.readers[i]);
                } else {
                    CopyVidFrom(lstate.readers[first_reader], lstate.readers[i]);
                }
            }
        }
    }

    idx_t num_rows = STANDARD_VECTOR_SIZE;
    for (auto& reader : lstate.readers) {
        if (IsNullPtr(reader)) continue;
        num_rows = std::min(num_rows, GetRowsNum(reader));
    }

    return num_rows;
}
void ReadHopFiltered::Execute(ClientContext& context, TableFunctionInput& input, DataChunk& output) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHopFiltered::Execute");

    ReadHopFilteredGlobalTableFunctionState& gstate = input.global_state->Cast<ReadHopFilteredGlobalTableFunctionState>();
    ReadHopFilteredLocalTableFunctionState& lstate = input.local_state->Cast<ReadHopFilteredLocalTableFunctionState>();

    std::string chunk_name = "Chunk RHF " + std::to_string(gstate.chunk_count);
    DUCKDB_GRAPHAR_LOG_DEBUG(chunk_name + ": Begin iteration");

    idx_t num_rows = FetchRowsNum<true>(gstate, lstate);
    DUCKDB_GRAPHAR_LOG_DEBUG(chunk_name + " num rows pred: " + std::to_string(num_rows));
    
    if (num_rows == 0) {
        std::lock_guard<std::mutex> guard(gstate.lock);
        while (!gstate.vertexes.empty() && num_rows == 0) {
            // lstate.cur_idx = gstate.MoveBaseReaders(lstate.cur_idx);
            num_rows = FetchRowsNum<false>(gstate, lstate);
        }
        // lstate.storage_state = gstate.storage_state;
    }

    DUCKDB_GRAPHAR_LOG_DEBUG(chunk_name + " num rows final: " + std::to_string(num_rows));

    if (num_rows > 0) {
        bool chunk_id_set = false;
        for (idx_t i = 0; i < lstate.readers.size(); i++) {
            if (IsNullPtr(lstate.readers[i])) {
                continue;
            }
            
            auto gc_result_final = GetChunk(lstate.readers[i], num_rows);
            lstate.cur_chunks[i] = std::move(gc_result_final.first);

            if (!chunk_id_set) {
                lstate.cur_chunk_id = gc_result_final.second;
                chunk_id_set = true;
            } else if (lstate.cur_chunk_id != gc_result_final.second) {
                throw InternalException("Desynchronization error: Property Groups returned different chunk IDs!");
            }

            for (idx_t j = 0; j < lstate.cur_chunks[i]->ColumnCount(); ++j) {
                if (!gstate.dst_column_found && gstate.special_dst.first == i && gstate.special_dst.second == j) {
                    continue;
                }
                output.data[gstate.global_projected_inds[i][j]].Reference(lstate.cur_chunks[i]->data[j]);
            }
        }
        
        if (chunk_id_set && GetResultIdx(lstate.cur_chunk_id) < gstate.next_hop_idx) {
            for (idx_t i = 0; i < num_rows; i++) {
                size_t v = lstate.cur_chunks[gstate.special_dst.first]->data[gstate.special_dst.second].GetValue(i).GetValue<int64_t>();

                // Need check uniq vertexes for 2 hop roots
                if (!gstate._vertexes.contains(v)) {
                    gstate.vertexes.push(v);
                    gstate._vertexes.insert(v);
                }
            }
        } else {
               DUCKDB_GRAPHAR_LOG_DEBUG(chunk_name + " chunk id set " + std::to_string(chunk_id_set) + " cur res idx" + std::to_string(GetResultIdx(lstate.cur_chunk_id)) + "next hop idx " + std::to_string(gstate.next_hop_idx)); 
        }
        
    }

    output.SetCapacity(num_rows);
    output.SetCardinality(num_rows);
    gstate.total_rows += num_rows;
    gstate.chunk_count++;

    if (num_rows == 0) {
        DUCKDB_GRAPHAR_LOG_DEBUG("One-hop unique size: " + std::to_string(gstate.vertexes.size()));
        std::string _temp = "_vertexes: {";
        for (auto v : gstate._vertexes) {
            _temp += std::to_string(v) + ",";
        }
        _temp += "}";
        DUCKDB_GRAPHAR_LOG_DEBUG(_temp);
        DUCKDB_GRAPHAR_LOG_DEBUG(gstate.vertexesToString());
    }
}

}  // namespace duckdb
