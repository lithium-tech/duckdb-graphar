#pragma once

#include "utils/benchmark.hpp"
#include "utils/func.hpp"
#include "utils/global_log_manager.hpp"

#include <arrow/c/bridge.h>

#include <duckdb/common/named_parameter_map.hpp>
#include <duckdb/function/table/arrow.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/extension_util.hpp>

#include <graphar/api/arrow_reader.h>
#include <graphar/api/high_level_reader.h>
#include <graphar/arrow/chunk_reader.h>
#include <graphar/chunk_info_reader.h>
#include <graphar/expression.h>
#include <graphar/fwd.h>
#include <graphar/reader_util.h>

#include <cassert>
#include <iostream>
#include <sstream>
#include <variant>

namespace duckdb {

using Reader = std::variant<graphar::VertexPropertyChunkInfoReader, graphar::AdjListChunkInfoReader,
                            graphar::AdjListPropertyChunkInfoReader>;

static graphar::Status next_chunk(Reader& reader) {
    return std::visit([](auto& r) { return r.next_chunk(); }, reader);
}

static graphar::Result<std::string> GetChunk(Reader& reader) {
    return std::visit([](auto& r) { return r.GetChunk(); }, reader);
}

static graphar::Status seek_vid(Reader& reader, graphar::IdType vid, std::string& filter_column) {
    return std::visit(
        [&](auto& r) {
            if (filter_column == GID_COLUMN_INTERNAL) {
                return r.seek(vid);
            } else if constexpr (requires { r.seek_src(vid); }) {
                if (filter_column == SRC_GID_COLUMN) {
                    return r.seek_src(vid);
                } else if (filter_column == DST_GID_COLUMN) {
                    return r.seek_dst(vid);
                } else {
                    return graphar::Status::TypeError("unknown filter_column value");
                }
            } else {
                return graphar::Status::TypeError("seek_vid is not implemented for this type of reader");
            }
        },
        reader);
}

template <typename ReadFinal>
class ReadBase;

class ReadVertices;
class ReadEdges;

class ReadBindData : public TableFunctionData {
public:
    ReadBindData() = default;
    vector<std::string> GetParams() { return params; }
    vector<std::string>& GetFlattenPropNames() { return flatten_prop_names; }
    vector<std::string>& GetFlattenPropTypes() { return flatten_prop_types; }

private:
    vector<vector<std::string>> prop_names;
    vector<std::string> flatten_prop_names;
    vector<vector<std::string>> prop_types;
    vector<std::string> flatten_prop_types;
    std::shared_ptr<graphar::GraphInfo> graph_info;
    std::string function_name;
    vector<std::string> params;
    graphar::PropertyGroupVector pgs;
    idx_t columns_to_remove = 0;

    template <typename ReadFinal>
    friend class ReadBase;
    friend class ReadVertices;
    friend class ReadEdges;
};

class ReadBaseGlobalTableFunctionState : public GlobalTableFunctionState {
private:
    idx_t chunk_count = 0;
    vector<std::shared_ptr<Reader>> readers;
    std::string function_name;
    vector<column_t> column_ids;

    std::string query_string;
    unique_ptr<Connection> conn;
    unique_ptr<QueryResult> cur_result;
    unique_ptr<DataChunk> cur_chunk;

    std::string filter_column;
    std::string filter_value;

    template <typename ReadFinal>
    friend class ReadBase;
    friend class ReadVertices;
    friend class ReadEdges;
};

template <typename ReadFinal>
class ReadBase {
public:
    template <typename TypeInfo>
    requires(std::is_same_v<TypeInfo, graphar::VertexInfo> || std::is_same_v<TypeInfo, graphar::EdgeInfo>)
    static void SetBindData(std::shared_ptr<graphar::GraphInfo> graph_info, const TypeInfo& type_info,
                            unique_ptr<ReadBindData>& bind_data, string function_name, idx_t columns_to_remove = 0,
                            idx_t pg_for_id = 0, vector<string> id_columns = {}) {
        DUCKDB_GRAPHAR_LOG_TRACE("ReadBase::SetBindData");
        bind_data->pgs = type_info.GetPropertyGroups();
        DUCKDB_GRAPHAR_LOG_DEBUG("pgs size " + std::to_string(bind_data->pgs.size()));
        bind_data->prop_types.resize(bind_data->pgs.size() + pg_for_id);
        bind_data->prop_names.resize(bind_data->prop_types.size());

        idx_t total_props_num = id_columns.size();
        for (idx_t i = 0; i < bind_data->pgs.size(); ++i) {
            int prop_num = bind_data->pgs[i]->GetProperties().size();
            total_props_num += prop_num;
            bind_data->prop_names[i + pg_for_id].reserve(prop_num);
            bind_data->prop_types[i + pg_for_id].reserve(prop_num);
        }
        DUCKDB_GRAPHAR_LOG_DEBUG("total_props_num: " + std::to_string(total_props_num));

        vector<std::string> names;
        names.reserve(total_props_num);
        bind_data->flatten_prop_types.reserve(total_props_num);

        for (auto& id_column : id_columns) {
            names.push_back(id_column);
            bind_data->prop_types[0].emplace_back("int64");
            bind_data->flatten_prop_types.emplace_back("int64");
            bind_data->prop_names[0].emplace_back(id_column);
        }

        for (idx_t i = 0; i < bind_data->pgs.size(); ++i) {
            for (auto p : bind_data->pgs[i]->GetProperties()) {
                auto type_name = std::move(p.type->ToTypeName());
                names.emplace_back(p.name);
                bind_data->prop_types[i + pg_for_id].emplace_back(type_name);
                bind_data->flatten_prop_types.emplace_back(type_name);
                bind_data->prop_names[i + pg_for_id].emplace_back(p.name);
            }
        }
        DUCKDB_GRAPHAR_LOG_DEBUG("Bind data filled");

        bind_data->function_name = function_name;
        bind_data->flatten_prop_names = std::move(names);
        bind_data->columns_to_remove = columns_to_remove;
        if constexpr (std::is_same_v<TypeInfo, graphar::VertexInfo>) {
            bind_data->params = {type_info.GetType()};
        } else {
            bind_data->params = {type_info.GetSrcType(), type_info.GetEdgeType(), type_info.GetDstType()};
        }

        bind_data->graph_info = graph_info;
        DUCKDB_GRAPHAR_LOG_TRACE("ReadBase::SetBindData finished");
    }

    static unique_ptr<FunctionData> Bind(ClientContext& context, TableFunctionBindInput& input,
                                         vector<LogicalType>& return_types, vector<string>& names) {
        return ReadFinal::Bind(context, input, return_types, names);
    }

    static std::shared_ptr<Reader> GetReader(ReadBaseGlobalTableFunctionState& gstate, ReadBindData& bind_data,
                                             idx_t ind, const std::string& filter_value,
                                             const std::string& filter_column, const std::string& filter_type) {
        return ReadFinal::GetReader(gstate, bind_data, ind, filter_value, filter_column, filter_type);
    }

    static void SetFilter(ReadBaseGlobalTableFunctionState& gstate, ReadBindData& bind_data, std::string& filter_value,
                          std::string& filter_column, std::string& filter_type) {
        ReadFinal::SetFilter(gstate, bind_data, filter_value, filter_column, filter_type);
    }

    static void SetQueryString(ReadBaseGlobalTableFunctionState& gstate) {
        gstate.query_string = "SELECT ";
        if (gstate.column_ids.size() == 1 and gstate.column_ids[0] == COLUMN_IDENTIFIER_ROW_ID) {
            gstate.query_string += "#1 ";
        } else {
            for (auto& column_id : gstate.column_ids) {
                gstate.query_string += "#" + std::to_string(column_id + 1) + ",";
            }
            gstate.query_string.pop_back();
            gstate.query_string += " ";
        }
        gstate.query_string += "FROM read_parquet($1)";
        if (gstate.filter_column != "") {
            gstate.query_string += " WHERE " + gstate.filter_column + " = " + gstate.filter_value;
        }
        gstate.query_string += ";";
    }

    static bool NextResult(ReadBaseGlobalTableFunctionState& gstate, bool is_first_result = false) {
        DUCKDB_GRAPHAR_LOG_TRACE("ReadBase::NextResult");
        ScopedTimer t("NextResult");
        vector<Value> path_list;
        for (auto& reader : gstate.readers) {
            if (!is_first_result && !next_chunk(*reader).ok()) {
                return false;
            }
            auto maybe_next_path = GetChunk(*reader);
            assert(!maybe_next_path.has_error());
            auto next_path = maybe_next_path.value();
            path_list.emplace_back(next_path);
        }
        t.print("GetChunk");
        Value path_list_val = Value::LIST(path_list);
        gstate.cur_result = std::move(gstate.conn->Query(gstate.query_string, path_list_val));
        t.print("read_parquet");
        DUCKDB_GRAPHAR_LOG_TRACE("ReadBase::NextResult finished");
        return true;
    }

    static unique_ptr<GlobalTableFunctionState> Init(ClientContext& context, TableFunctionInitInput& input) {
        DUCKDB_GRAPHAR_LOG_TRACE("Init started");
        bool time_logging = GraphArSettings::is_time_logging(context);

        ScopedTimer t("StateInit");

        auto bind_data = input.bind_data->Cast<ReadBindData>();

        DUCKDB_GRAPHAR_LOG_TRACE(bind_data.function_name + "::Init");

        if (time_logging) {
            t.print("cast");
        }

        ReadBaseGlobalTableFunctionState gstate;

        DUCKDB_GRAPHAR_LOG_DEBUG("Init global state");

        gstate.function_name = bind_data.function_name;
        gstate.column_ids = input.column_ids;
        gstate.readers.resize(bind_data.prop_types.size());

        DatabaseInstance fake_db;
        gstate.conn = std::move(make_uniq<Connection>(*context.db));

        DUCKDB_GRAPHAR_LOG_DEBUG("readers num: " + std::to_string(gstate.readers.size()));

        std::string filter_value, filter_column, filter_type;
        if (input.filters) {
            DUCKDB_GRAPHAR_LOG_DEBUG("Found filters");

            if (input.filters->filters.size() > 1) {
                throw NotImplementedException("Multiple filters are not supported");
            }
            auto filter_id = input.filters->filters.begin()->first;
            auto filter_index = input.column_ids[filter_id];
            auto& filter = input.filters->filters.begin()->second;
            if (filter->filter_type != TableFilterType::CONSTANT_COMPARISON) {
                throw NotImplementedException("Only constant filters are supported");
            }
            auto filter_expr = filter->ToString(" ");
            if (filter_expr[1] != '=') {
                throw NotImplementedException("Only equality filters are supported");
            }

            filter_value = filter_expr.substr(2);

            filter_column = bind_data.flatten_prop_names[filter_index];
            filter_type = bind_data.flatten_prop_types[filter_index];
            gstate.filter_column = filter_column;
            gstate.filter_value = filter_value;
            DUCKDB_GRAPHAR_LOG_DEBUG("filter column: " + filter_column + " filter type: " + filter_type +
                                     " filter value: " + filter_value);
        }
        if (time_logging) {
            t.print("filter parsing");
        }

        for (idx_t i = 0; i < gstate.readers.size(); i++) {
            gstate.readers[i] = GetReader(gstate, bind_data, i, filter_value, filter_column, filter_type);
        }
        if (time_logging) {
            t.print("readers creation");
        }
        SetFilter(gstate, bind_data, filter_value, filter_column, filter_type);
        if (time_logging) {
            t.print("filter setting");
        }

        SetQueryString(gstate);
        t.print("SetQueryString");
        for (idx_t i = 0; i < gstate.readers.size(); i++) {
            NextResult(gstate, true);
        }

        if (time_logging) {
            t.print("NextResult");
        }

        DUCKDB_GRAPHAR_LOG_DEBUG("::Init Done");
        if (time_logging) {
            t.print();
        }

        return make_uniq<ReadBaseGlobalTableFunctionState>(std::move(gstate));
    }

    static void Execute(ClientContext& context, TableFunctionInput& input, DataChunk& output) {
        bool time_logging = GraphArSettings::is_time_logging(context);

        ScopedTimer t("Execute");

        DUCKDB_GRAPHAR_LOG_DEBUG("::Execute Cast state");

        ReadBaseGlobalTableFunctionState& gstate = input.global_state->Cast<ReadBaseGlobalTableFunctionState>();

        DUCKDB_GRAPHAR_LOG_DEBUG("Chunk " + std::to_string(gstate.chunk_count) + ": Begin iteration");
        gstate.cur_chunk = std::move(gstate.cur_result->Fetch());
        bool all_read = false;
        if (!gstate.cur_chunk) {
            if (!NextResult(gstate)) {
                all_read = true;
            } else {
                gstate.cur_chunk = std::move(gstate.cur_result->Fetch());
            }
        }
        if (all_read || !gstate.cur_chunk) {
            output.SetCapacity(0);
            output.SetCardinality(0);
        } else {
            output.Reference(*gstate.cur_chunk);
            gstate.chunk_count++;
        }
        DUCKDB_GRAPHAR_LOG_DEBUG("Chunk size: " + std::to_string(output.size()));
        if (time_logging) {
            t.print();
        }
    }

    static void Register(DatabaseInstance& db) { ExtensionUtil::RegisterFunction(db, ReadFinal::GetFunction()); }
    static TableFunction GetFunction() { return ReadFinal::GetFunction(); }
    static TableFunction GetScanFunction() { return ReadFinal::GetScanFunction(); }
};
}  // namespace duckdb
