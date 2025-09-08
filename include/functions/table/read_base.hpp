#pragma once

#include "utils/benchmark.hpp"
#include "utils/custom_chunk_info_reader.hpp"
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

using Reader = std::variant<graphar::VertexPropertyChunkInfoReader, graphar::CustomAdjListChunkInfoReader,
                            graphar::CustomAdjListPropertyChunkInfoReader>;

static graphar::Status next_chunk(Reader& reader) {
    return std::visit([](auto& r) { return r.next_chunk(); }, reader);
}

static graphar::Result<std::string> GetChunk(Reader& reader) {
    return std::visit([](auto& r) { return r.GetChunk(); }, reader);
}

static graphar::Status seek(Reader& reader, graphar::IdType id) {
    DUCKDB_GRAPHAR_LOG_TRACE("seek");
    return std::visit(
        [&](auto& r) {
            if constexpr (requires { r.seek(id); }) {
                return r.seek(id);
            } else {
                return graphar::Status::TypeError("seek is not implemented for this type of reader");
            }
        },
        reader);
}

static graphar::Status seek_src(Reader& reader, graphar::IdType id, std::pair<graphar::IdType, graphar::IdType> offset_pair) {
    DUCKDB_GRAPHAR_LOG_TRACE("seek_src");
    return std::visit(
        [&](auto& r) {
            if constexpr (requires { r.seek_src(id, offset_pair); }) {
                return r.seek_src(id, offset_pair);
            } else {
                return graphar::Status::TypeError("seek_src is not implemented for this type of reader");
            }
        },
        reader);
}

static graphar::Status seek_dst(Reader& reader, graphar::IdType id, std::pair<graphar::IdType, graphar::IdType> offset_pair) {
    DUCKDB_GRAPHAR_LOG_TRACE("seek_dst");
    return std::visit(
        [&](auto& r) {
            if constexpr (requires { r.seek_dst(id, offset_pair); }) {
                return r.seek_dst(id, offset_pair);
            } else {
                return graphar::Status::TypeError("seek_dst is not implemented for this type of reader");
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
    idx_t chunk_size = 0;

    template <typename ReadFinal>
    friend class ReadBase;
    friend class ReadVertices;
    friend class ReadEdges;
};

class QueryStringConstructor {
public:
    enum class QueryType { FIRST, MIDDLE, LAST, SINGLE };
    std::string GetQueryString(std::string& projected_columns_string, QueryType query_type,
                               graphar::IdType filter_val = 0) {
        std::ostringstream ss;
        ss << SQL_SELECT_CLAUSE << " " << projected_columns_string << " " << SQL_FROM_CLAUSE << " ";
        switch (file_type) {
            case graphar::FileType::PARQUET:
                ss << READ_PARQUET_FUNCTION;
                break;
            case graphar::FileType::CSV:
                throw NotImplementedException("CSV file format is not supported yet");
                break;
            case graphar::FileType::JSON:
                throw NotImplementedException("JSON file format is not supported yet");
                break;
            case graphar::FileType::ORC:
                throw NotImplementedException("ORC file format is not supported yet");
                break;
            default:
                throw NotImplementedException("Unknown file type");
        }
        ss << "($1, " << FILE_ROW_NUMBER_CLAUSE << "=true)";
        if (query_type == QueryType::FIRST) {
            ss << " " << SQL_WHERE_CLAUSE << " " << FILE_ROW_NUMBER_CLAUSE << " >= $2";
        } else if (query_type == QueryType::LAST) {
            ss << " " << SQL_WHERE_CLAUSE << " " << FILE_ROW_NUMBER_CLAUSE << " < $2";
        } else if (query_type == QueryType::SINGLE) {
            ss << " " << SQL_WHERE_CLAUSE << " " << FILE_ROW_NUMBER_CLAUSE << " >= $2 AND " << FILE_ROW_NUMBER_CLAUSE
               << " < $3";
        }
        ss << ";";
        return ss.str();
    }

    void SetFileType(graphar::FileType file_type_) { file_type = file_type_; }

private:
    static constexpr std::string_view SQL_SELECT_CLAUSE = "SELECT";
    static constexpr std::string_view SQL_FROM_CLAUSE = "FROM";
    static constexpr std::string_view SQL_WHERE_CLAUSE = "WHERE";
    static constexpr std::string_view READ_PARQUET_FUNCTION = "read_parquet";
    static constexpr std::string_view FILE_ROW_NUMBER_CLAUSE = "file_row_number";

private:
    graphar::FileType file_type;
};

class ReadBaseGlobalTableFunctionState : public GlobalTableFunctionState {
    idx_t chunk_count = 0;
    vector<std::shared_ptr<Reader>> readers;
    std::string function_name;
    vector<column_t> column_ids;
    std::pair<row_t, row_t> filter_range = {-1, -1};

    idx_t chunk_size = 0;

    QueryStringConstructor query_string_constructor;
    vector<std::string> projected_columns_strings;
    unique_ptr<Connection> conn;
    vector<unique_ptr<QueryResult>> current_queries_results;
    vector<unique_ptr<DataChunk>> current_results_chunks;
    vector<idx_t> num_read_rows;
    idx_t total_rows = 0;

    std::string filter_column;
    std::string filter_value;

    idx_t read_rows = 0;

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
        const auto prop_types_size = bind_data->prop_types.size();
        bind_data->prop_names.resize(prop_types_size);

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
        bind_data->chunk_size = type_info.GetChunkSize();
        if (bind_data->chunk_size == 0) {
            throw IOException("Chunk size can not be 0");
        }
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
        gstate.total_rows = gstate.filter_range.second - gstate.filter_range.first;
    }

    static bool NextResult(ReadBaseGlobalTableFunctionState& gstate, bool is_first_result = false) {
        DUCKDB_GRAPHAR_LOG_TRACE("ReadBase::NextResult");
        QueryStringConstructor::QueryType query_type = QueryStringConstructor::QueryType::MIDDLE;
        if (!gstate.filter_column.empty()) {
            if (gstate.read_rows == 0) {
                if (gstate.chunk_size >= gstate.total_rows) {
                    query_type = QueryStringConstructor::QueryType::SINGLE;
                } else {
                    query_type = QueryStringConstructor::QueryType::FIRST;
                }
            } else if (gstate.read_rows + gstate.chunk_size >= gstate.total_rows) {
                query_type = QueryStringConstructor::QueryType::LAST;
            }
        }
        for (idx_t i = 0; i < gstate.readers.size(); ++i) {
            auto& reader = gstate.readers[i];
            if (!is_first_result && !next_chunk(*reader).ok()) {
                return false;
            }
            auto maybe_next_path = GetChunk(*reader);
            if (maybe_next_path.has_error()) {
                throw std::runtime_error("Failed to get chunk: " + maybe_next_path.error().message());
            }
            auto next_path = maybe_next_path.value();
            auto query_string = std::move(
                gstate.query_string_constructor.GetQueryString(gstate.projected_columns_strings[i], query_type));
            unique_ptr<QueryResult> query_result = nullptr;
            switch (query_type) {
                case QueryStringConstructor::QueryType::MIDDLE:
                    query_result = std::move(gstate.conn->Query(query_string, Value(std::move(next_path))));
                    break;
                case QueryStringConstructor::QueryType::FIRST:
                    query_result =
                        std::move(gstate.conn->Query(query_string, Value(std::move(next_path)),
                                                     Value::BIGINT(gstate.filter_range.first % gstate.chunk_size)));
                    break;
                case QueryStringConstructor::QueryType::LAST:
                    query_result =
                        std::move(gstate.conn->Query(query_string, Value(std::move(next_path)),
                                                     Value::BIGINT(gstate.filter_range.second % gstate.chunk_size)));
                    break;
                case QueryStringConstructor::QueryType::SINGLE:
                    query_result =
                        std::move(gstate.conn->Query(query_string, Value(std::move(next_path)),
                                                     Value::BIGINT(gstate.filter_range.first % gstate.chunk_size),
                                                     Value::BIGINT(gstate.filter_range.second % gstate.chunk_size)));
                    break;
            }
            if (query_result->HasError()) {
                throw std::runtime_error("Failed to execute query: " + query_result->GetError());
            }
            gstate.current_queries_results[i] = std::move(query_result);
        }
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
        gstate.query_string_constructor.SetFileType(
            bind_data.graph_info->GetVertexInfoByIndex(0)->GetPropertyGroupByIndex(0)->GetFileType());
        gstate.chunk_size = bind_data.chunk_size;

        std::string filter_value, filter_column, filter_type;
        if (input.filters) {
            DUCKDB_GRAPHAR_LOG_DEBUG("Found filters");

            if (input.filters->filters.size() > 1) {
                std::cout << "filters: " << std::endl;
                for (auto& filter : input.filters->filters) {
                    std::cout << filter.second->ToString(" ") << std::endl;
                }
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

        const auto prop_types_size = bind_data.prop_types.size();
        vector<idx_t> columns_pref_num(prop_types_size + 1);
        columns_pref_num[0] = 0;
        for (idx_t i = 0; i < prop_types_size; i++) {
            columns_pref_num[i + 1] = columns_pref_num[i] + bind_data.prop_types[i].size();
        }

        gstate.projected_columns_strings.reserve(prop_types_size);
        gstate.readers.reserve(prop_types_size);
        if (gstate.column_ids.empty() ||
            gstate.column_ids.size() == 1 && gstate.column_ids[0] == COLUMN_IDENTIFIER_ROW_ID) {
            DUCKDB_GRAPHAR_LOG_DEBUG("Returning any column");
            gstate.projected_columns_strings.emplace_back("#1");
            gstate.readers.emplace_back(GetReader(gstate, bind_data, 0, filter_value, filter_column, filter_type));
        } else {
            DUCKDB_GRAPHAR_LOG_DEBUG("Returning specific columns");
            idx_t it = 0;
            vector<vector<idx_t>> column_ids_split_by_reader(prop_types_size);
            for (idx_t i = 1; i < columns_pref_num.size(); ++i) {
                while (it < gstate.column_ids.size() && gstate.column_ids[it] < columns_pref_num[i]) {
                    column_ids_split_by_reader[i - 1].emplace_back(gstate.column_ids[it] - columns_pref_num[i - 1]);
                    it++;
                }
            }
            for (idx_t i = 0; i < prop_types_size; ++i) {
                auto& vec = column_ids_split_by_reader[i];
                if (vec.empty()) {
                    continue;
                }
                std::ostringstream column_ids_str;
                for (idx_t j = 0; j < vec.size(); ++j) {
                    column_ids_str << "#";
                    if (i != 0) {
                        column_ids_str << std::to_string(vec[j] + bind_data.columns_to_remove + 1);
                    } else {
                        column_ids_str << std::to_string(vec[j] + 1);
                    }
                    if (j != vec.size() - 1) {
                        column_ids_str << ", ";
                    }
                }
                gstate.projected_columns_strings.emplace_back(std::move(column_ids_str).str());
                DUCKDB_GRAPHAR_LOG_DEBUG("projected columns: " + gstate.projected_columns_strings.back());
                gstate.readers.emplace_back(GetReader(gstate, bind_data, i, filter_value, filter_column, filter_type));
            }
        }
        if (time_logging) {
            t.print("readers creation");
        }

        gstate.current_queries_results.resize(prop_types_size);
        gstate.current_results_chunks.resize(prop_types_size);
        gstate.num_read_rows.resize(prop_types_size);

        DatabaseInstance fake_db;
        gstate.conn = std::move(make_uniq<Connection>(*context.db));

        DUCKDB_GRAPHAR_LOG_DEBUG("readers num: " + std::to_string(gstate.readers.size()));

        if (time_logging) {
            t.print("readers creation");
        }
        SetFilter(gstate, bind_data, filter_value, filter_column, filter_type);
        if (time_logging) {
            t.print("filter setting");
        }

        NextResult(gstate, true);

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
        idx_t num_rows = STANDARD_VECTOR_SIZE;
        if (!gstate.filter_column.empty() && gstate.read_rows == gstate.total_rows) {
            num_rows = 0;
        }
        for (idx_t i = 0; i < gstate.readers.size() && num_rows; ++i) {
            if (!gstate.current_results_chunks[i] ||
                gstate.current_results_chunks[i]->size() == gstate.num_read_rows[i]) {
                gstate.current_results_chunks[i] = gstate.current_queries_results[i]->Fetch();
                if (!gstate.current_results_chunks[i]) {
                    if (!NextResult(gstate, false)) {
                        num_rows = 0;
                        break;
                    }
                    gstate.current_results_chunks[i] = gstate.current_queries_results[i]->Fetch();
                    if (!gstate.current_results_chunks[i]) {
                        num_rows = 0;
                        break;
                    }
                }
                gstate.num_read_rows[i] = 0;
            }
        }
        if (num_rows > 0) {
            for (idx_t i = 0; i < gstate.readers.size(); i++) {
                num_rows = std::min(num_rows, gstate.current_results_chunks[i]->size() - gstate.num_read_rows[i]);
            }
            idx_t it = 0;
            for (idx_t i = 0; i < gstate.readers.size(); i++) {
                for (idx_t j = 0; j < gstate.current_results_chunks[i]->ColumnCount(); j++) {
                    Vector vec_slice(gstate.current_results_chunks[i]->data[j], gstate.num_read_rows[i],
                                     gstate.num_read_rows[i] + num_rows);
                    output.data[it++].Reference(vec_slice);
                }
                gstate.num_read_rows[i] += num_rows;
            }
        }

        output.SetCapacity(num_rows);
        output.SetCardinality(num_rows);
        gstate.chunk_count++;
        gstate.read_rows += num_rows;
        DUCKDB_GRAPHAR_LOG_DEBUG("Chunk size: " + std::to_string(num_rows));
        if (time_logging) {
            t.print();
        }
    }

    static void Register(DatabaseInstance& db) { ExtensionUtil::RegisterFunction(db, ReadFinal::GetFunction()); }
    static TableFunction GetFunction() { return ReadFinal::GetFunction(); }
    static TableFunction GetScanFunction() { return ReadFinal::GetScanFunction(); }
};
}  // namespace duckdb
