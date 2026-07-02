
#include "functions/table/read_edges.hpp"
#include "functions/table/read_hop_filtered.hpp"
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
// GetBindData
//-------------------------------------------------------------------
void ReadHopFiltered::SetBindData(std::shared_ptr<graphar::GraphInfo> graph_info, std::shared_ptr<graphar::EdgeInfo> edge_info,
                          unique_ptr<ReadHopFilteredBindData>& bind_data) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHopFiltered::SetBindData")
    unique_ptr<ReadBindData> base_ptr = std::move(bind_data);

    ReadBase::SetBindData(graph_info, edge_info, base_ptr, "read_hop_filtered", 0, 1, {SRC_GID_COLUMN, DST_GID_COLUMN});

    if (base_ptr) {
        auto* derived = dynamic_cast<ReadHopFilteredBindData*>(base_ptr.get());
        if (derived) {
            base_ptr.release();
            bind_data.reset(derived);
        }
    }
}
//-------------------------------------------------------------------
// Bind
//-------------------------------------------------------------------
unique_ptr<FunctionData> ReadHopFiltered::Bind(ClientContext& context, TableFunctionBindInput& input,
                                         vector<LogicalType>& return_types, vector<string>& names) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHopFiltered::Bind Parse parameters");

    const auto file_path = StringValue::Get(input.inputs[0]);
    const std::string src_type = StringValue::Get(input.named_parameters.at("src"));
    const std::string dst_type = StringValue::Get(input.named_parameters.at("dst"));
    const std::string e_type = StringValue::Get(input.named_parameters.at("type"));

    duckdb::vector<duckdb::Value> duck_vids;
    auto vids_entry = input.named_parameters.find("vids");  
    if (vids_entry == input.named_parameters.end()) {  
        auto vid_entry = input.named_parameters.find("vid");  
        if (vid_entry != input.named_parameters.end() && !vid_entry->second.IsNull()) {  
            duck_vids = {vid_entry->second};
        } else {
            throw BinderException("Expecting a named parameter vids or vid");
        }
    } else {
        duck_vids = ListValue::GetChildren(vids_entry->second);
        if (duck_vids.empty()) {
            throw BinderException("Expecting non empty vids");
        }
    }

    auto bind_data = make_uniq<ReadHopFilteredBindData>();
    bind_data->vids.resize(duck_vids.size());
    
    std::string _vids_str = "";
    for (size_t i = 0; i < duck_vids.size(); ++i) {
        bind_data->vids[i] = IntegerValue::Get(duck_vids[i]);
        if (!_vids_str.empty()) {
            _vids_str += ',';
        }
        _vids_str += std::to_string(bind_data->vids[i]);
    }
    const int64_t vid = bind_data->vids[0];

    std::string filter_string;
    if (input.named_parameters.contains("filter")) {
        filter_string = StringValue::Get(input.named_parameters.at("filter"));
    } 

    DUCKDB_GRAPHAR_LOG_DEBUG(src_type + "(" + _vids_str + ")--" +
                             e_type + (filter_string != "" ? "WHERE " + filter_string : "") + "->" +
                             dst_type + "\nLoad Graph Info and Edge Info");

    DUCKDB_GRAPHAR_LOG_DEBUG("file path " + file_path);
    auto maybe_graph_info = graphar::GraphInfo::Load(file_path);
    if (maybe_graph_info.has_error()) {
        throw IOException("Failed to load graph info from path: %s", file_path);
    }
    auto graph_info = maybe_graph_info.value();

    auto edge_info = graph_info->GetEdgeInfo(src_type, e_type, dst_type);
    if (!edge_info) {
        throw BinderException("Edges of this type are not found");
    }

    DUCKDB_GRAPHAR_LOG_DEBUG("Fill bind data");

    SetBindData(graph_info, edge_info, bind_data);

    DUCKDB_GRAPHAR_LOG_DEBUG("After SetBindData");
    bind_data->vid_range = std::make_pair(vid, vid + 1);
    bind_data->filter_column = SRC_GID_COLUMN;
    bind_data->graph_info_path = file_path;

    names = bind_data->flatten_prop_names;
    std::transform(bind_data->flatten_prop_types.begin(), bind_data->flatten_prop_types.end(),
                   std::back_inserter(return_types),
                   [](const auto& return_type) { return GraphArFunctions::graphArT2duckT(return_type); });
    

    DUCKDB_GRAPHAR_LOG_DEBUG("Bind finish");
    return bind_data;
}
//-------------------------------------------------------------------
// GetBaseReader
//-------------------------------------------------------------------
BaseReaderPtr ReadHopFiltered::GetBaseReader(ClientContext& context, ReadHopFilteredGlobalTableFunctionState& gstate, idx_t ind,
                                     const std::string& filter_column) {
    DUCKDB_GRAPHAR_LOG_TRACE("ReadHopFiltered::GetBaseReader");
    auto conn = std::make_shared<Connection>(*context.db);
    auto query_base_reader = QueryChunkReader::Make(std::move(conn), gstate.query_string, gstate.vertexes[gstate.cur_ind]);
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
TableFunction ReadHopFiltered::GetFunction() {
    TableFunction read_hop("read_hop_filtered", {LogicalType::VARCHAR}, Execute, Bind);
    read_hop.init_global = Init;
    read_hop.init_local = InitLocal;

    read_hop.named_parameters["src"] = LogicalType::VARCHAR;
    read_hop.named_parameters["dst"] = LogicalType::VARCHAR;
    read_hop.named_parameters["type"] = LogicalType::VARCHAR;
    read_hop.named_parameters["vids"] = LogicalType::LIST(LogicalType::INTEGER);
    read_hop.named_parameters["vid"] = LogicalType::INTEGER;

    read_hop.filter_pushdown = false;
    read_hop.projection_pushdown = true;
    read_hop.statistics = GetStatistics;
    read_hop.pushdown_complex_filter = PushdownComplexFilter;

    return read_hop;
}
//-------------------------------------------------------------------
// GetScanFunction
//-------------------------------------------------------------------
TableFunction ReadHopFiltered::GetScanFunction() {
    TableFunction read_hop({}, Execute, Bind);
    read_hop.init_global = Init;
    read_hop.init_local = InitLocal;

    read_hop.filter_pushdown = false;
    read_hop.projection_pushdown = true;
    read_hop.statistics = GetStatistics;
    read_hop.pushdown_complex_filter = PushdownComplexFilter;

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


    // std::string _temp = "";
    bool found_dst_column = false;
    for (size_t i = 0; i < gstate.column_ids.size(); i++) {
        if (bind_data.flatten_prop_names[gstate.column_ids[i]] == DST_GID_COLUMN) {
            gstate.dstColumn = i;
            found_dst_column = true;
            break;
        }
        // _temp += bind_data.flatten_prop_names[gstate.column_ids[i]] + "<" + std::to_string(gstate.column_ids[i])+ "> ";
    }
    if (!found_dst_column) {
        throw IOException("Not found dst column (" + DST_GID_COLUMN + ") in query");
    }
    // DUCKDB_GRAPHAR_LOG_DEBUG("ReadHopFiltered::GlobalState::test " + _temp);

    // DUCKDB_GRAPHAR_LOG_DEBUG("ReadHopFiltered::GlobalState::dstColumn " + std::to_string(gstate.dstColumn));

    gstate.GenerateQuery();

    DUCKDB_GRAPHAR_LOG_DEBUG("ReadHopFiltered::GlobalState::function_name " + bind_data.function_name);
    DUCKDB_GRAPHAR_LOG_DEBUG("ReadHopFiltered::GlobalState::id_columns_num " + std::to_string(bind_data.id_columns_num));
    std::string _temp = "";
    for (idx_t j = 0; j < gstate.column_ids.size(); j++) {
        _temp += std::to_string(gstate.column_ids[j]) + " ";
    }
    DUCKDB_GRAPHAR_LOG_DEBUG("ReadHopFiltered::GlobalState::column_ids(" + std::to_string(gstate.column_ids.size()) + ") " + _temp);
    // _temp = "";
    // for (idx_t j = 0; j < bind_data.flatten_prop_names.size(); j++) {
    //     _temp += bind_data.flatten_prop_names[j] + " ";
    // }
    // DUCKDB_GRAPHAR_LOG_DEBUG("ReadHopFiltered::GlobalState::flatten_prop_names(" + std::to_string(bind_data.flatten_prop_names.size()) + ") " + _temp);
    DUCKDB_GRAPHAR_LOG_DEBUG("ReadHopFiltered::GlobalState::filter_column " + bind_data.filter_column);
    _temp = "";
    for (idx_t j = 0; j < bind_data.params.size(); j++) {
        _temp += bind_data.params[j] + " ";
    }
    DUCKDB_GRAPHAR_LOG_DEBUG("ReadHopFiltered::GlobalState::params(" + std::to_string(bind_data.params.size()) + ") " + _temp);
    DUCKDB_GRAPHAR_LOG_DEBUG("ReadHopFiltered::GlobalState::query_filter " + bind_data.query_filter);
    DUCKDB_GRAPHAR_LOG_DEBUG("ReadHopFiltered::GlobalState::graph_info_path " + bind_data.graph_info_path);
    DUCKDB_GRAPHAR_LOG_DEBUG("ReadHopFiltered::GlobalState::dstColumn " + std::to_string(gstate.dstColumn));


    auto offset_pair =
        graphar::util::GetAdjListOffsetOfVertex(*std::get_if<std::shared_ptr<graphar::EdgeInfo>>(&bind_data.type_info),
                                                bind_data.graph_info->GetPrefix(),
                                                graphar::AdjListType::ordered_by_source, bind_data.vid_range.first)
            .value();

    gstate.vertexes.reserve((offset_pair.second - offset_pair.first + 1) * bind_data.vids.size());
    gstate._vertexes.reserve((offset_pair.second - offset_pair.first + 1) * bind_data.vids.size());
    for (const auto& vid : bind_data.vids) {
        gstate.vertexes.push_back(vid);
        gstate._vertexes.insert(vid);
    }
    
    gstate.cur_ind = 0;
    gstate.next_hop_ind = gstate.vertexes.size();

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
            std::visit([&](const auto& ptr) {
                DUCKDB_GRAPHAR_LOG_DEBUG("Generate reader (" + std::to_string(i) + "): " +
                demangle(typeid(ptr).name()));
            }, gstate.base_readers[i]);
        }
    }

    DUCKDB_GRAPHAR_LOG_DEBUG("ReadHopFilteredHop::Init global_projected_inds");

    for (idx_t i = 0; i < gstate.global_projected_inds.size(); i++) {
        std::string _temp = "";
        for (idx_t j = 0; j < gstate.global_projected_inds[i].size(); j++) {
            _temp += std::to_string(gstate.global_projected_inds[i][j]) + " ";
        }
        DUCKDB_GRAPHAR_LOG_DEBUG("ReadHop::Init global_projected_inds " + std::to_string(i) + ": " + _temp);
    }

    DUCKDB_GRAPHAR_LOG_DEBUG("readers num: " + std::to_string(gstate.base_readers.size()));

    // if (filter_column != "") {
    //     DUCKDB_GRAPHAR_LOG_TRACE("Filters found");
    //     auto vid_range = bind_data.vid_range;
    //     const auto vertex_num = GetCountClass::GetCount(gstate.type_info, bind_data.GetGraphInfo()->GetPrefix());
    //     graphar::IdType zero = 0;
    //     vid_range.first = std::max(zero, vid_range.first);
    //     vid_range.second = std::min(vertex_num, vid_range.second);
    //     if (vid_range.first >= vid_range.second) {
    //         throw IOException("Invalid filter range: " + std::to_string(vid_range.first) + " > " +
    //                           std::to_string(vid_range.second));
    //     }
    //     // for (idx_t ind = 0; ind < prop_types_size; ++ind) {
    //     //     if (IsNullPtr(gstate.base_readers[ind])) {
    //     //         continue;
    //     //     }
    //     //     SetFilter(context, gstate, ind, vid_range, filter_column);
    //     // }
    // }

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

    lstate.cur_ind = gstate.cur_ind;
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

    // Need check readers not empty?
    bool no_more_rows = std::visit([&](auto&& r) -> bool { return r->NoMoreRows(); }, lstate.readers[0]);
    DUCKDB_GRAPHAR_LOG_DEBUG("no more rows: " + std::to_string(no_more_rows));
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
    DUCKDB_GRAPHAR_LOG_DEBUG("num rows pred: " + std::to_string(num_rows));

    if (num_rows == 0) {
        std::lock_guard<std::mutex> lock(gstate.mtx);
        while (lstate.cur_ind < gstate.vertexes.size() && num_rows == 0) {
            lstate.cur_ind = gstate.MoveBaseReaders(lstate.cur_ind);
            
            DUCKDB_GRAPHAR_LOG_DEBUG("cur vertex: " + std::to_string(gstate.vertexes[lstate.cur_ind])); 
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
        for (idx_t i = 0; i < lstate.readers.size(); i++) {
            if (IsNullPtr(lstate.readers[i])) {
                continue;
            }
            lstate.cur_chunks[i] = std::move(GetChunk(lstate.readers[i], num_rows));
            // for (idx_t j = 0; j < lstate.cur_chunks[i]->ColumnCount(); j++) {
            //     output.data[j].Reference(lstate.cur_chunks[i]->data[j]);
            // }
            output.Reference(*lstate.cur_chunks[i]);
        }
        if (lstate.storage_state) {
            for (idx_t i = 0; i < num_rows; i++) {
                auto v = output.data[gstate.dstColumn].GetValue(i).GetValue<graphar::IdType>();
                // Need check uniq vertexes for 2 hop roots
                if (!gstate._vertexes.contains(v)) {
                    // Need use iters with '<' operator for no double move of base_reader
                    gstate.vertexes.push_back(v);
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
        DUCKDB_GRAPHAR_LOG_DEBUG("One-hop unique size: " + std::to_string(gstate.vertexes.size()) + ", reserve (real size) " +
                                 std::to_string(gstate.vertexes.capacity()));
    }
}

}  // namespace duckdb
