
#include "functions/table/read_hop_filtered.hpp"

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
unique_ptr<FunctionData> ReadHopFiltered::Bind(ClientContext& context, TableFunctionBindInput& input,
                                               vector<LogicalType>& return_types, vector<string>& names) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHopFiltered::Bind")
    const bool is_catalog_mode = HopBase::IsCatalogMode(input);

    auto bind_data = make_uniq<ReadHopFilteredBindData>();

    if (is_catalog_mode) {
        HopBase::SetBindDataByEdgeTable(context, input, *bind_data);
    } else {
        HopBase::SetBindDataByGraphPath(context, input, *bind_data);
        bind_data->graph_info_path = StringValue::Get(input.inputs[0]);
    }

    HopBase::SetBindDataVids(input, *bind_data);

    ReadBase::SetBindData(bind_data->graph_info, bind_data->edge_info,
                          reinterpret_cast<unique_ptr<ReadBindData>&>(bind_data), GetFunctionName(), 0, 1,
                          {SRC_GID_COLUMN, DST_GID_COLUMN});

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
BaseReaderPtr ReadHopFiltered::GetBaseReader(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate,
                                             idx_t ind, const std::string& filter_column,
                                             std::shared_ptr<graphar::SharedChunkCounter> counter) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHopFiltered::GetBaseReader");

    return ConvertBaseReader(graphar::VidsChunkReader::Make(), counter);
}
//-------------------------------------------------------------------
// GetReader
//-------------------------------------------------------------------
ReaderPtr ReadHopFiltered::GetReader(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate,
                                     ReadBaseLocalTableFunctionState& lstate, idx_t ind,
                                     const std::string& filter_column) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHopFiltered::GetReader");
    auto& lstate_hop = lstate.Cast<ReadHopFilteredLocalTableFunctionState>();

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
    for (size_t i = 0; i < filters.size(); ++i) {
        if (i) read_bind_data.query_filter += " AND ";
        read_bind_data.query_filter += filters[i]->ToString();
    }
    DUCKDB_GRAPHAR_LOG_DEBUG("filters<" + std::to_string(filters.size()) + ">:" + read_bind_data.query_filter);

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
unique_ptr<GlobalTableFunctionState> ReadHopFiltered::InitWrapper(ClientContext& context,
                                                                  TableFunctionInitInput& input) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHopFiltered::Init");

    auto bind_data = input.bind_data->Cast<ReadHopFilteredBindData>();
    bool dst_column_found = true;

    if (std::find(input.column_ids.begin(), input.column_ids.end(), bind_data.dst_column_idx) ==
        input.column_ids.end()) {
        dst_column_found = false;
        input.column_ids.push_back(bind_data.dst_column_idx);
    }

    auto base_gstate_ptr = Init(context, input);
    auto& base_gstate = base_gstate_ptr->Cast<ReadBaseGlobalTableFunctionState>();
    auto gstate_ptr = std::make_unique<ReadHopFilteredGlobalTableFunctionState>(base_gstate);
    auto& gstate = *gstate_ptr;

    gstate.column_ids = input.column_ids;
    gstate.dst_column_found = dst_column_found;

    {
        std::ostringstream ss;
        ss << "ReadHopFiltered::Init: Cids " << input.column_ids.size() << ": ";
        for (const auto& cid : gstate.column_ids) {
            ss << ' ' << cid;
        }
        DUCKDB_GRAPHAR_LOG_WARN(ss.str());
    }

    HopBase::SetGlobalState(bind_data, gstate);

    gstate.query_filter = bind_data.query_filter;
    gstate.graph_info_path = bind_data.graph_info_path;

    for (auto& base_readers : gstate.base_readers) {
        for (auto& base_reader : base_readers) {
            std::visit(
                [&gstate_ptr](auto& r) {
                    if constexpr (requires { r->Init(gstate_ptr.get()); }) {
                        r->Init(gstate_ptr.get());
                    } else {
                        throw InternalException("Init not implemented for this reader " +
                                                DemangleTypeName(typeid(r).name()));
                    }
                },
                base_reader);
        }
    }

    {
        std::ostringstream ss;
        ss << "ReadHopFiltered::Init: GPI";
        for (const auto& proj_inds : gstate.global_projected_inds) {
            ss << proj_inds.size() << "[";
            for (const auto& ind : proj_inds) {
                ss << ind << " ";
            }
            ss << "]\n";
        }
        DUCKDB_GRAPHAR_LOG_WARN(ss.str());
    }

    {
        std::ostringstream ss;
        ss << "ReadHopFiltered::Init: LPI";
        for (const auto& proj_inds : gstate.local_projected_inds) {
            ss << proj_inds.size() << "[";
            for (const auto& ind : proj_inds) {
                ss << ind << " ";
            }
            ss << "]\n";
        }
        DUCKDB_GRAPHAR_LOG_WARN(ss.str());
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
    auto edge_reader =
        DuckEdgeReader::Make(conn, bind_data.GetFullTableName(), gstate.graph_info_path, gstate.edge_info, gstate.query_filter);
    if (edge_reader.has_error()) {
        throw InternalException("Failed to create edge reader: |" + edge_reader.status().message() + "|");
    }

    lstate.edge_reader = edge_reader.value();

    std::ostringstream ss;
    ss << "ReadHopFiltered::InitLocal: readers";

    for (idx_t i = 0; i < prop_types_size; ++i) {
        if (gstate.global_projected_inds[i].empty()) {
            continue;
        }
        lstate.readers[i] = std::move(GetReader(context.client, gstate, lstate, i, gstate.filter_column));
        vector<column_t> projs;
        projs.reserve(gstate.global_projected_inds[i].size());
        for (const auto& proj_ind : gstate.global_projected_inds[i]) {
            projs.push_back(gstate.column_ids[proj_ind]);
        }
        SelectColumns(lstate.readers[i], projs);
        ss << "\n" << i << ":";
        for (const auto& ind : projs) {
            ss << ' ' << ind;
        }
        ss << "\n";
    }
    DUCKDB_GRAPHAR_LOG_WARN(ss.str());

    return lstate_ptr;
}
//-------------------------------------------------------------------
// Execute
//-------------------------------------------------------------------
template <bool notLocked>
idx_t ReadHopFiltered::FetchRowsNum(ReadHopFilteredGlobalTableFunctionState& gstate,
                                    ReadHopFilteredLocalTableFunctionState& lstate) {
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

    ReadHopFilteredGlobalTableFunctionState& gstate =
        input.global_state->Cast<ReadHopFilteredGlobalTableFunctionState>();
    ReadHopFilteredLocalTableFunctionState& lstate = input.local_state->Cast<ReadHopFilteredLocalTableFunctionState>();

    std::string chunk_name = "Chunk RHF " + std::to_string(gstate.chunk_count);
    DUCKDB_GRAPHAR_LOG_DEBUG(chunk_name + ": Begin iteration");

    idx_t num_rows = FetchRowsNum<true>(gstate, lstate);
    DUCKDB_GRAPHAR_LOG_DEBUG(chunk_name + " num rows pred: " + std::to_string(num_rows));

    if (num_rows == 0) {
        std::lock_guard<std::mutex> guard(gstate.lock);
        while (!gstate.vertexes.empty() && num_rows == 0) {
            num_rows = FetchRowsNum<false>(gstate, lstate);
        }
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
                DUCKDB_GRAPHAR_LOG_DEBUG(chunk_name + " try i" + std::to_string(i) + " j" + std::to_string(j) + ": " + std::to_string(gstate.global_projected_inds[i][j]));
                output.data[gstate.global_projected_inds[i][j]].Reference(lstate.cur_chunks[i]->data[j]);
            }
        }

        if (chunk_id_set && GetResultIdx(lstate.cur_chunk_id) < gstate.next_hop_idx) {
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
        } else {
            DUCKDB_GRAPHAR_LOG_DEBUG(chunk_name + " chunk id set " + std::to_string(chunk_id_set) + " cur res idx" +
                                     std::to_string(GetResultIdx(lstate.cur_chunk_id)) + "next hop idx " +
                                     std::to_string(gstate.next_hop_idx));
        }
    }

    output.SetCapacity(num_rows);
    output.SetCardinality(num_rows);
    gstate.total_rows += num_rows;
    gstate.chunk_count++;
}

}  // namespace duckdb
