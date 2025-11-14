#pragma once

#include "readers/duck_arrow_chunk_reader.hpp"
#include "readers/duck_chunk_reader.hpp"
#include "utils/benchmark.hpp"
#include "utils/func.hpp"
#include "utils/global_log_manager.hpp"

#include <arrow/c/bridge.h>

#include <duckdb/common/named_parameter_map.hpp>
#include <duckdb/function/table/arrow.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>

#include <graphar/api/arrow_reader.h>
#include <graphar/api/high_level_reader.h>
#include <graphar/arrow/chunk_reader.h>
#include <graphar/expression.h>
#include <graphar/fwd.h>
#include <graphar/graph_info.h>
#include <graphar/reader_util.h>

#include <filesystem>
#include <iostream>
#include <sstream>
#include <variant>

namespace duckdb {

using Reader = std::variant<graphar::DuckVertexPropertyArrowChunkReader, graphar::DuckAdjListArrowChunkReader,
                            graphar::DuckAdjListPropertyArrowChunkReader, graphar::DuckVertexPropertyChunkReader,
                            graphar::DuckAdjListChunkReader, graphar::DuckAdjListPropertyChunkReader>;

template <typename SomeReader>
std::shared_ptr<Reader> ConvertReader(graphar::Result<std::shared_ptr<SomeReader>> maybe_reader) {
    if (maybe_reader.has_error()) {
        throw InternalException("Error converting reader: " + maybe_reader.error().message());
    }
    Reader result = SomeReader(std::move(*maybe_reader.value()));
    return std::make_shared<Reader>(std::move(result));
}

static unique_ptr<DataChunk> GetChunk(Reader& reader, int64_t num_rows) {
    DUCKDB_GRAPHAR_LOG_TRACE("GetChunk");
    return std::visit(
        [&num_rows](auto& r) {
            auto maybe_chunk = r.GetChunk(num_rows);
            if (maybe_chunk.has_error()) {
                throw InternalException("Error getting chunk: " + maybe_chunk.status().message());
            }
            return std::move(maybe_chunk.value());
        },
        reader);
}

static idx_t ReserveRowsToRead(Reader& reader) {
    return std::visit([&](auto& r) { return r.ReserveRowsToRead(); }, reader);
}

static void FilterByRange(Reader& reader, std::pair<int64_t, int64_t> vid_range, const std::string& filter_column) {
    return std::visit([&](auto& r) { r.FilterByRange(vid_range, filter_column); }, reader);
}

static void SelectColumns(Reader& reader, std::vector<column_t> proj_cols) {
    return std::visit([&](auto& r) { r.SelectColumns(proj_cols); }, reader);
}

using TypeInfoPtr = std::variant<std::shared_ptr<graphar::VertexInfo>, std::shared_ptr<graphar::EdgeInfo>>;

static const graphar::PropertyGroupVector& GetPropertyGroups(TypeInfoPtr& type_info) {
    return std::visit([&](auto& t) -> const graphar::PropertyGroupVector& { return t->GetPropertyGroups(); },
                      type_info);
}

static std::string GetVertexTypeName(TypeInfoPtr& type_info, const std::string& column_name) {
    return std::visit(
        [&](auto& t) {
            if constexpr (requires { t->GetType(); }) {
                return t->GetType();
            } else if constexpr (requires { t->GetSrcType(); }) {
                if (column_name == SRC_GID_COLUMN) {
                    return t->GetSrcType();
                } else {
                    return t->GetDstType();
                }
            }
        },
        type_info);
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
    const std::shared_ptr<graphar::GraphInfo>& GetGraphInfo() const { return graph_info; }

private:
    vector<vector<std::string>> prop_names;
    vector<std::string> flatten_prop_names;
    vector<vector<std::string>> prop_types;
    vector<std::string> flatten_prop_types;
    std::shared_ptr<graphar::GraphInfo> graph_info;
    std::string function_name;
    vector<std::string> params;
    graphar::PropertyGroupVector pgs;
    idx_t id_columns_num = 0;
    idx_t pg_for_id = 0;

    std::pair<graphar::IdType, graphar::IdType> vid_range = {-1, -1};
    std::string filter_column;

    TypeInfoPtr type_info;

    template <typename ReadFinal>
    friend class ReadBase;
    friend class ReadVertices;
    friend class ReadEdges;
};

class ReadBaseGlobalTableFunctionState : public GlobalTableFunctionState {
private:
    graphar::PropertyGroupVector pgs;
    vector<vector<std::string>> prop_names;
    vector<vector<std::string>> prop_types;
    idx_t chunk_count = 0;
    idx_t total_props_num = 0;
    vector<std::shared_ptr<Reader>> readers;
    vector<unique_ptr<DataChunk>> cur_chunks;
    std::pair<int64_t, int64_t> filter_range = {-1, -1};
    std::string function_name;
    int64_t total_rows = 0;
    vector<column_t> column_ids;
    vector<vector<column_t>> global_projected_inds;
    idx_t id_columns_num = 0;

    std::shared_ptr<DuckParquetFileReader> file_reader;

    template <typename ReadFinal>
    friend class ReadBase;
    friend class ReadVertices;
    friend class ReadEdges;
};

template <typename ReadFinal>
class ReadBase {
public:
    static void SetBindData(std::shared_ptr<graphar::GraphInfo> graph_info, TypeInfoPtr type_info,
                            unique_ptr<ReadBindData>& bind_data, string function_name, idx_t id_columns_num = 0,
                            idx_t pg_for_id = 0, vector<string> id_columns = {}) {
        DUCKDB_GRAPHAR_LOG_TRACE("ReadBase::SetBindData");
        if (std::filesystem::path(graph_info->GetPrefix()).is_relative()) {
            throw IOException(
                "Using relative path as prefix is not supported. Please use an absolute path or just remove this "
                "field.");
        }
        bind_data->pgs = GetPropertyGroups(type_info);
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
        bind_data->id_columns_num = id_columns_num;
        bind_data->pg_for_id = pg_for_id;
        bind_data->type_info = type_info;
        if (std::holds_alternative<std::shared_ptr<graphar::VertexInfo>>(type_info)) {
            auto vertex_info = *std::get_if<std::shared_ptr<graphar::VertexInfo>>(&type_info);
            bind_data->params = {vertex_info->GetType()};
        } else {
            auto edge_info = *std::get_if<std::shared_ptr<graphar::EdgeInfo>>(&type_info);
            bind_data->params = {edge_info->GetSrcType(), edge_info->GetEdgeType(), edge_info->GetDstType()};
        }

        bind_data->graph_info = graph_info;
        DUCKDB_GRAPHAR_LOG_TRACE("ReadBase::SetBindData finished");
    }

    static unique_ptr<FunctionData> Bind(ClientContext& context, TableFunctionBindInput& input,
                                         vector<LogicalType>& return_types, vector<string>& names) {
        return ReadFinal::Bind(context, input, return_types, names);
    }

    static std::shared_ptr<Reader> GetReader(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate,
                                             ReadBindData& bind_data, idx_t ind, const std::string& filter_column) {
        return ReadFinal::GetReader(context, gstate, bind_data, ind, filter_column);
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
        gstate.id_columns_num = bind_data.id_columns_num;
        gstate.pgs = bind_data.pgs;
        gstate.column_ids = input.column_ids;

        const auto prop_types_size = bind_data.prop_types.size();
        vector<idx_t> columns_pref_num(prop_types_size + 1);
        columns_pref_num[0] = 0;
        for (idx_t i = 0; i < prop_types_size; i++) {
            columns_pref_num[i + 1] = columns_pref_num[i] + bind_data.prop_types[i].size();
        }

        gstate.file_reader = std::make_shared<DuckParquetFileReader>(std::make_shared<Connection>(*context.db));

        const auto& filter_column = bind_data.filter_column;

        gstate.prop_names = std::move(bind_data.prop_names);
        gstate.prop_types = std::move(bind_data.prop_types);
        vector<vector<column_t>> local_projected_inds(prop_types_size);
        gstate.global_projected_inds.resize(prop_types_size);
        gstate.readers.resize(prop_types_size);
        if (gstate.column_ids.empty() ||
            gstate.column_ids.size() == 1 && gstate.column_ids[0] == COLUMN_IDENTIFIER_ROW_ID) {
            DUCKDB_GRAPHAR_LOG_DEBUG("Returning any column");
            local_projected_inds[0].emplace_back(0);
            gstate.readers[0] = GetReader(context, gstate, bind_data, 0, filter_column);
            gstate.global_projected_inds[0].emplace_back(0);
            SelectColumns(*gstate.readers[0], local_projected_inds[0]);
        } else {
            DUCKDB_GRAPHAR_LOG_DEBUG("Returning specific columns");
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
                gstate.readers[i] = std::move(GetReader(context, gstate, bind_data, i, filter_column));
                SelectColumns(*gstate.readers[i], local_projected_inds[i]);
            }
        }

        DUCKDB_GRAPHAR_LOG_DEBUG("readers num: " + std::to_string(gstate.readers.size()));

        if (filter_column != "") {
            DUCKDB_GRAPHAR_LOG_TRACE("Filters found");
            auto vid_range = bind_data.vid_range;
            const auto vertex_num = GraphArFunctions::GetVertexNum(
                bind_data.graph_info, GetVertexTypeName(bind_data.type_info, filter_column));
            graphar::IdType zero = 0;
            vid_range.first = std::max(zero, vid_range.first);
            vid_range.second = std::min(vertex_num - 1, vid_range.second);
            if (vid_range.first > vid_range.second) {
                throw IOException("Invalid filter range: " + std::to_string(vid_range.first) + " > " +
                                  std::to_string(vid_range.second));
            }
            for (auto& reader : gstate.readers) {
                if (!reader) {
                    continue;
                }
                FilterByRange(*reader, vid_range, filter_column);
            }
        }
        if (time_logging) {
            t.print("filter setting");
        }

        gstate.prop_names = std::move(bind_data.prop_names);
        gstate.prop_types = std::move(bind_data.prop_types);
        gstate.cur_chunks.resize(prop_types_size);

        if (time_logging) {
            t.print("additional info");
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
        for (auto& reader : gstate.readers) {
            if (!reader || !num_rows) {
                continue;
            }
            num_rows = std::min(num_rows, ReserveRowsToRead(*reader));
        }
        DUCKDB_GRAPHAR_LOG_DEBUG("num rows final: " + std::to_string(num_rows));

        if (num_rows > 0) {
            for (idx_t i = 0; i < gstate.readers.size(); i++) {
                if (!gstate.readers[i]) {
                    continue;
                }
                gstate.cur_chunks[i] = std::move(GetChunk(*gstate.readers[i], num_rows));
                for (idx_t j = 0; j < gstate.cur_chunks[i]->ColumnCount(); j++) {
                    output.data[gstate.global_projected_inds[i][j]].Reference(gstate.cur_chunks[i]->data[j]);
                }
            }
        }

        output.SetCapacity(num_rows);
        output.SetCardinality(num_rows);
        gstate.total_rows += num_rows;
        DUCKDB_GRAPHAR_LOG_DEBUG("Size of chunk: " + std::to_string(num_rows) +
                                 " Total size: " + std::to_string(gstate.total_rows))
        if (time_logging) {
            t.print();
        }
        gstate.chunk_count++;
    }

    static void Register(ExtensionLoader& loader) { loader.RegisterFunction(ReadFinal::GetFunction()); }
    static TableFunction GetFunction() { return ReadFinal::GetFunction(); }
    static TableFunction GetScanFunction() { return ReadFinal::GetScanFunction(); }
};
}  // namespace duckdb
