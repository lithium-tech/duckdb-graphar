#include "functions/table/read_hop.hpp"

#include "functions/table/read_edges.hpp"
#include "storage/graphar_catalog.hpp"
#include "storage/graphar_schema_entry.hpp"
#include "utils/benchmark.hpp"
#include "utils/func.hpp"

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
// Bind
//-------------------------------------------------------------------
unique_ptr<FunctionData> ReadHop::Bind(ClientContext& context, TableFunctionBindInput& input,
                                       vector<LogicalType>& return_types, vector<string>& names) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHop::Bind");

    const bool is_catalog_mode = HopBase::IsCatalogMode(input);

    auto bind_data = make_uniq<ReadHopBindData>();

    if (is_catalog_mode) {
        HopBase::SetBindDataByEdgeTable(context, input, *bind_data);
    } else {
        HopBase::SetBindDataByGraphPath(context, input, *bind_data);
    }

    HopBase::SetBindDataVids(input, *bind_data);
    HopBase::SetBindDataFilter(*bind_data);

    auto graph_info = bind_data->graph_info;
    auto edge_info = bind_data->edge_info;
    unique_ptr<ReadBindData> base_bind_data = std::move(bind_data);
    ReadBase::SetBindData(graph_info, edge_info, base_bind_data, GetFunctionName(), 0, 1,
                          {SRC_GID_COLUMN, DST_GID_COLUMN});
    bind_data.reset(static_cast<ReadHopBindData*>(base_bind_data.release()));

    names = bind_data->GetFlattenPropNames();
    const auto& fpt = bind_data->GetFlattenPropTypes();
    std::transform(fpt.begin(), fpt.end(), std::back_inserter(return_types),
                   [](const auto& return_type) { return GraphArFunctions::graphArT2duckT(return_type); });

    HopBase::SetBindDataDstIdx(names, *bind_data);

    return std::move(bind_data);
}
//-------------------------------------------------------------------
// GetBaseReader
//-------------------------------------------------------------------
BaseReaderPtr ReadHop::GetBaseReader(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate, idx_t ind,
                                     const std::string& filter_column,
                                     std::shared_ptr<graphar::SharedChunkCounter> counter) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHop::GetBaseReader");
    return ReadEdges::GetBaseReader(context, gstate, ind, filter_column, counter);
}
//-------------------------------------------------------------------
// SetFilter
//-------------------------------------------------------------------
void ReadHop::SetFilter(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate, idx_t ind,
                        const vector<std::pair<int64_t, int64_t>>& vid_ranges, const std::string& filter_column) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHop::SetFilter");
    ReadEdges::SetFilter(context, gstate, ind, vid_ranges, filter_column);
}
//-------------------------------------------------------------------
// GetReader
//-------------------------------------------------------------------
ReaderPtr ReadHop::GetReader(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate,
                             ReadBaseLocalTableFunctionState& lstate, idx_t ind, const std::string& filter_column) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHop::GetReader");
    return ReadEdges::GetReader(context, gstate, lstate, ind, filter_column);
}
//-------------------------------------------------------------------
// GetFunction
//-------------------------------------------------------------------
TableFunctionSet ReadHop::GetFunctions() {
    TableFunctionSet read_hop(GetFunctionName());

    TableFunction read_hop_default({LogicalType::VARCHAR}, Execute, Bind);
    SetTableFuncionParams(read_hop_default);
    read_hop.AddFunction(read_hop_default);

    return read_hop;
}
//-------------------------------------------------------------------
// GetScanFunction
//-------------------------------------------------------------------
TableFunction ReadHop::GetScanFunction() {
    TableFunction read_hop(GetFunctionName(), {}, Execute, Bind);
    SetTableFuncionParams(read_hop);
    return read_hop;
}
//-------------------------------------------------------------------
// Init
//-------------------------------------------------------------------
unique_ptr<GlobalTableFunctionState> ReadHop::InitWrapper(ClientContext& context, TableFunctionInitInput& input) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHop::InitWrapper");

    auto bind_data = input.bind_data->Cast<ReadHopBindData>();
    bool dst_column_found = true;

    if (std::find(input.column_ids.begin(), input.column_ids.end(), bind_data.dst_column_idx) ==
        input.column_ids.end()) {
        dst_column_found = false;
        input.column_ids.push_back(bind_data.dst_column_idx);
    }

    auto base_gstate_ptr = Init(context, input);
    auto& base_gstate = base_gstate_ptr->Cast<ReadBaseGlobalTableFunctionState>();
    auto gstate_ptr = std::make_unique<ReadHopGlobalTableFunctionState>(base_gstate);
    auto& gstate = *gstate_ptr;
    gstate.dst_column_found = dst_column_found;

    HopBase::SetGlobalState(bind_data, gstate);

    gstate.vertexes = std::queue<graphar::IdType>();
    gstate.cur_idx = gstate.next_hop_idx - 1;

    return gstate_ptr;
}
//-------------------------------------------------------------------
// InitLocal
//-------------------------------------------------------------------
unique_ptr<LocalTableFunctionState> ReadHop::InitLocalWrapper(ExecutionContext& context, TableFunctionInitInput& input,
                                                              GlobalTableFunctionState* gstate_ptr) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHop::InitLocalWrapper");
    auto& gstate = gstate_ptr->Cast<ReadHopGlobalTableFunctionState>();

    auto base_lstate_ptr = InitLocal(context, input, gstate_ptr);
    auto& base_lstate = base_lstate_ptr->Cast<ReadBaseLocalTableFunctionState>();

    auto lstate_ptr = std::make_unique<ReadHopLocalTableFunctionState>(base_lstate);
    lstate_ptr->cur_idx = gstate.cur_idx;
    lstate_ptr->storage_state = gstate.storage_state;
    return lstate_ptr;
}
//-------------------------------------------------------------------
// Execute
//-------------------------------------------------------------------
template <bool notLocked>
idx_t ReadHop::FetchRowsNum(ReadHopGlobalTableFunctionState& gstate, ReadHopLocalTableFunctionState& lstate) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHop::FetchRowsNum");

    bool needs_new_file = false;
    for (auto& reader : lstate.readers) {
        if (IsNullPtr(reader)) continue;
        if (CheckIfNewFileNeeded(reader)) {
            needs_new_file = true;
        }
    }

    if (needs_new_file) {
        if constexpr (notLocked) {
            std::lock_guard<std::mutex> guard(gstate.lock);
            for (auto& reader : lstate.readers) {
                if (IsNullPtr(reader)) continue;
                AcquirePathUnderLock(reader);
            }
            lstate.storage_state = gstate.storage_state;
        } else {
            for (auto& reader : lstate.readers) {
                if (IsNullPtr(reader)) continue;
                AcquirePathUnderLock(reader);
            }
            lstate.storage_state = gstate.storage_state;
        }
    }

    idx_t num_rows = STANDARD_VECTOR_SIZE;
    for (auto& reader : lstate.readers) {
        if (IsNullPtr(reader)) continue;
        num_rows = std::min(num_rows, GetRowsNum(reader));
    }
    return num_rows;
}

void ReadHop::Execute(ClientContext& context, TableFunctionInput& input, DataChunk& output) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHop::Execute");

    ReadHopGlobalTableFunctionState& gstate = input.global_state->Cast<ReadHopGlobalTableFunctionState>();
    ReadHopLocalTableFunctionState& lstate = input.local_state->Cast<ReadHopLocalTableFunctionState>();

    std::string chunk_name = "RH Chunk " + std::to_string(gstate.chunk_count);
    DUCKDB_GRAPHAR_LOG_DEBUG(chunk_name + ": Begin iteration");

    idx_t num_rows = FetchRowsNum<true>(gstate, lstate);

    if (num_rows == 0) {
        std::lock_guard<std::mutex> guard(gstate.lock);
        while (!gstate.vertexes.empty() && num_rows == 0) {
            lstate.cur_idx = gstate.MoveBaseReaders(lstate.cur_idx);
            for (auto i = 0; i < lstate.readers.size(); ++i) {
                if (!gstate.base_readers[i].empty()) {
                    Reset(lstate.readers[i]);
                }
            }
            num_rows = FetchRowsNum<false>(gstate, lstate);
        }
    }

    DUCKDB_GRAPHAR_LOG_DEBUG(chunk_name + "num rows final: " + std::to_string(num_rows));

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

            for (idx_t j = 0; j < lstate.cur_chunks[i]->ColumnCount(); j++) {
                if (!gstate.dst_column_found && gstate.special_dst.first == i && gstate.special_dst.second == j) {
                    continue;
                }
                output.data[gstate.global_projected_inds[i][j]].Reference(lstate.cur_chunks[i]->data[j]);
            }
        }

        if (lstate.storage_state) {
            std::lock_guard<std::mutex> guard(gstate.lock);
            for (idx_t i = 0; i < num_rows; i++) {
                size_t v = lstate.cur_chunks[gstate.special_dst.first]
                               ->data[gstate.special_dst.second]
                               .GetValue(i)
                               .GetValue<int64_t>();

                // Need check uniq vertexes for 2 hop roots
                if (!gstate._vertexes.contains(v)) {
                    gstate.vertexes.push(v);
                    gstate._vertexes.insert(v);
                }
            }
        }
    }

    output.SetCapacity(num_rows);
    output.SetCardinality(num_rows);
    gstate.total_rows += num_rows;
    gstate.chunk_count++;
}

}  // namespace duckdb
