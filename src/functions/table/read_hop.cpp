#include "functions/table/read_hop.hpp"

#include "functions/table/read_edges.hpp"
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
void ReadHop::SetBindData(unique_ptr<ReadHopBindData>& bind_data) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHop::SetBindData")

    const int64_t vid = bind_data->vids[0];
    bind_data->vid_range = std::make_pair(vid, vid + 1);
    bind_data->filter_column = SRC_GID_COLUMN;

    ReadBase::SetBindData(bind_data->graph_info, bind_data->edge_info, reinterpret_cast<unique_ptr<ReadBindData>&>(bind_data), GetFunctionName(), 0, 1, {SRC_GID_COLUMN, DST_GID_COLUMN});
}
//-------------------------------------------------------------------
// Bind
//-------------------------------------------------------------------
unique_ptr<FunctionData> ReadHop::Bind(ClientContext& context, TableFunctionBindInput& input,
                                       vector<LogicalType>& return_types, vector<string>& names) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHop::Bind");

    const bool is_catalog_mode = HopBase::IsCatalogMode(input);

    auto bind_data = make_uniq<ReadHopBindData>();

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
BaseReaderPtr ReadHop::GetBaseReader(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate, idx_t ind,
                                     const std::string& filter_column) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHop::GetBaseReader");

    BaseReaderPtr base_reader = ReadEdges::GetBaseReader(context, gstate, ind, filter_column);
    return base_reader;
}
//-------------------------------------------------------------------
// SetFilter
//-------------------------------------------------------------------
void ReadHop::SetFilter(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate, idx_t ind,
                        const std::pair<int64_t, int64_t>& vid_range, const std::string& filter_column) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHop::SetFilter");
    auto edge_info = *std::get_if<std::shared_ptr<graphar::EdgeInfo>>(&gstate.type_info);
    if (!edge_info) {
        throw InternalException("Failed to get edge info");
    }
    const auto& prefix = gstate.graph_info->GetPrefix();

    // TODO: Check adj list
    auto vertex_num_file_suffix = edge_info->GetVerticesNumFilePath(graphar::AdjListType::ordered_by_source).value();
    std::string vertex_num_file_path = prefix + vertex_num_file_suffix;

    auto vertex_num = GetCountClass::GetCount(edge_info->GetSrcType(), vertex_num_file_path);

    if (vid_range.first < 0 || vid_range.first >= vertex_num || vid_range.second <= 0 ||
        vid_range.second > vertex_num) {
        throw BinderException("Invalid filter vertex id range");
    }

    FilterByRangeEdge(gstate.base_readers[ind], vid_range, filter_column, edge_info, prefix);
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
// GetStatistics
//-------------------------------------------------------------------
unique_ptr<BaseStatistics> ReadHop::GetStatistics(ClientContext& context, const FunctionData* bind_data,
                                                  column_t column_index) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHop::GetStatistics");
    auto read_bind_data = bind_data->Cast<ReadHopBindData>();
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
void ReadHop::PushdownComplexFilter(ClientContext& context, LogicalGet& get, FunctionData* bind_data,
                                    vector<unique_ptr<Expression>>& filters) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHop::PushdownComplexFilter");
    return ReadEdges::PushdownComplexFilter(context, get, bind_data, filters);
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
    read_hop.init_global = ReadHop::Init;
    read_hop.init_local = ReadHop::InitLocal;

    read_hop.filter_pushdown = false;
    read_hop.projection_pushdown = true;
    read_hop.statistics = ReadHop::GetStatistics;
    read_hop.pushdown_complex_filter = ReadHop::PushdownComplexFilter;

    return read_hop;
}
//-------------------------------------------------------------------
// Init
//-------------------------------------------------------------------
unique_ptr<GlobalTableFunctionState> ReadHop::Init(ClientContext& context, TableFunctionInitInput& input) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHop::Init");

    auto bind_data = input.bind_data->Cast<ReadHopBindData>();

    auto gstate_ptr = make_uniq<ReadHopGlobalTableFunctionState>();
    auto& gstate = *gstate_ptr;

    HopBase::SetGlobalState(bind_data, gstate);
    gstate.vertexes.pop();

    gstate.function_name = bind_data.function_name;
    gstate.id_columns_num = bind_data.id_columns_num;
    gstate.pgs = bind_data.pgs;
    gstate.column_ids = input.column_ids;
    gstate.filter_column = bind_data.filter_column;
    gstate.type_info = bind_data.type_info;
    gstate.graph_info = bind_data.graph_info;
    gstate.params = bind_data.params;

    gstate.dst_column_found = false;
    for (size_t i = 0; i < input.column_ids.size(); i++) {
        if (input.column_ids[i] == gstate.dst_column_idx) {
            gstate.dst_column_idx = i;
            gstate.dst_column_found = true;
            break;
        }
    }
    if (!gstate.dst_column_found) {
        gstate.dst_column_idx = gstate.column_ids.size();
        gstate.column_ids.push_back(gstate.dst_column_idx);
    }

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
            if (!gstate.dst_column_found && column_i == gstate.dst_column_idx) {
                gstate.special_dst = {i, gstate.global_projected_inds[i].size() - 1};
            }
        }

        for (idx_t i = 0; i < prop_types_size; ++i) {
            if (local_projected_inds[i].empty()) {
                continue;
            }

            gstate.base_readers[i] = std::move(GetBaseReader(context, gstate, i, filter_column));
        }
    }
    DUCKDB_GRAPHAR_LOG_DEBUG("ReadHop::GlobalState::special_dst {" + std::to_string(gstate.special_dst.first) + ", " + std::to_string(gstate.special_dst.second) + "}");
    DUCKDB_GRAPHAR_LOG_DEBUG("ReadHop::Init dstColumn " + std::to_string(gstate.dst_column_idx));
    DUCKDB_GRAPHAR_LOG_DEBUG("readers num: " + std::to_string(gstate.base_readers.size()));

    if (filter_column != "") {
        DUCKDB_GRAPHAR_LOG_TRACE("Filters found");
        auto vid_range = bind_data.vid_range;
        const auto vertex_num = GetCountClass::GetCount(gstate.type_info, bind_data.GetGraphInfo()->GetPrefix());
        graphar::IdType zero = 0;
        vid_range.first = std::max(zero, vid_range.first);
        vid_range.second = std::min(vertex_num, vid_range.second);
        if (vid_range.first >= vid_range.second) {
            throw IOException("Invalid filter range: " + std::to_string(vid_range.first) + " > " +
                              std::to_string(vid_range.second));
        }
        for (idx_t ind = 0; ind < prop_types_size; ++ind) {
            if (IsNullPtr(gstate.base_readers[ind])) {
                continue;
            }
            SetFilter(context, gstate, ind, vid_range, filter_column);
        }
    }

    gstate.local_projected_inds = std::move(local_projected_inds);

    DUCKDB_GRAPHAR_LOG_DEBUG("::Init Done");

    return gstate_ptr;
}
//-------------------------------------------------------------------
// InitLocal
//-------------------------------------------------------------------
unique_ptr<LocalTableFunctionState> ReadHop::InitLocal(ExecutionContext& context, TableFunctionInitInput& input,
                                                       GlobalTableFunctionState* gstate_ptr) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHop::InitLocal");
    auto bind_data = input.bind_data->Cast<ReadHopBindData>();

    auto lstate_ptr = make_uniq<ReadHopLocalTableFunctionState>();
    auto& lstate = *lstate_ptr;
    auto& gstate = gstate_ptr->Cast<ReadHopGlobalTableFunctionState>();

    lstate.cur_ind = gstate.cur_idx;
    lstate.file_reader = std::make_shared<DuckParquetFileReader>(std::make_shared<Connection>(*context.client.db));
    const auto prop_types_size = gstate.prop_types.size();
    lstate.cur_chunks.resize(prop_types_size);
    lstate.readers.resize(prop_types_size);

    for (idx_t i = 0; i < prop_types_size; ++i) {
        if (gstate.local_projected_inds[i].empty()) {
            continue;
        }
        lstate.readers[i] = std::move(GetReader(context.client, gstate, lstate, i, gstate.filter_column));
        SelectColumns(lstate.readers[i], gstate.local_projected_inds[i]);
    }

    lstate.storage_state = gstate.storage_state;

    return lstate_ptr;
}
//-------------------------------------------------------------------
// Execute
//-------------------------------------------------------------------
void ReadHop::Execute(ClientContext& context, TableFunctionInput& input, DataChunk& output) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHop::Execute");

    ReadHopGlobalTableFunctionState& gstate = input.global_state->Cast<ReadHopGlobalTableFunctionState>();
    ReadHopLocalTableFunctionState& lstate = input.local_state->Cast<ReadHopLocalTableFunctionState>();

    std::string chunk_name = "Chunk " + std::to_string(gstate.chunk_count);
    DUCKDB_GRAPHAR_LOG_DEBUG(chunk_name + ": Begin iteration");

    idx_t num_rows = STANDARD_VECTOR_SIZE;

    // Need check readers not empty?
    bool no_more_rows = std::visit([&](auto&& r) -> bool { return r->NoMoreRows(); }, lstate.readers[0]);
    DUCKDB_GRAPHAR_LOG_DEBUG(chunk_name + " no more rows: " + std::to_string(no_more_rows));
    // State of readers equal ? (can use state of readers[0] for all) ?
    if (no_more_rows) {
        // no_more_rows -> try get chunk by new state of base_reader -> readers must use equal state of base_readers
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
    DUCKDB_GRAPHAR_LOG_DEBUG(chunk_name + "num rows pred: " + std::to_string(num_rows));

    if (num_rows == 0) {
        std::lock_guard<std::mutex> lock(gstate.mtx);
        while (!gstate.vertexes.empty() && num_rows == 0) {
            lstate.cur_ind = gstate.MoveBaseReaders(lstate.cur_ind);
            
            num_rows = STANDARD_VECTOR_SIZE;
            for (auto& reader : lstate.readers) {
                if (IsNullPtr(reader) || !num_rows) {
                    continue;
                }
                
                idx_t reserve_rows = ReserveRowsToRead(reader);
                DUCKDB_GRAPHAR_LOG_DEBUG(chunk_name + "num rows reserved: " + std::to_string(reserve_rows));
                num_rows = std::min(num_rows, reserve_rows);
            }
            DUCKDB_GRAPHAR_LOG_DEBUG(chunk_name + "num rows: " + std::to_string(num_rows));
        }
        lstate.storage_state = gstate.storage_state;
    }

    DUCKDB_GRAPHAR_LOG_DEBUG(chunk_name + "num rows final: " + std::to_string(num_rows));

    if (num_rows > 0) {
        for (idx_t i = 0; i < lstate.readers.size(); i++) {
            if (IsNullPtr(lstate.readers[i])) {
                continue;
            }
            lstate.cur_chunks[i] = std::move(GetChunk(lstate.readers[i], num_rows));
            for (idx_t j = 0; j < lstate.cur_chunks[i]->ColumnCount(); j++) {
                if (gstate.dst_column_found || gstate.special_dst.first != i || gstate.special_dst.second != j) {
                    output.data[gstate.global_projected_inds[i][j]].Reference(lstate.cur_chunks[i]->data[j]);
                }
            }
        }
        if (lstate.storage_state) {
            const auto proj = gstate.dst_column_idx;

            for (idx_t i = 0; i < num_rows; i++) {
                size_t v;
                if (gstate.dst_column_found) {
                    v = output.data[proj].GetValue(i).GetValue<int64_t>();
                } else {
                    v = lstate.cur_chunks[gstate.special_dst.first]->data[gstate.special_dst.second].GetValue(i).GetValue<int64_t>();
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
    DUCKDB_GRAPHAR_LOG_DEBUG(chunk_name + ": Size of chunk: " + std::to_string(num_rows) +
                             " Total size: " + std::to_string(gstate.total_rows))

    gstate.chunk_count++;

    if (num_rows == 0) {
        DUCKDB_GRAPHAR_LOG_DEBUG("One-hop unique size: " + std::to_string(gstate.vertexes.size()));
    }
}

}  // namespace duckdb
