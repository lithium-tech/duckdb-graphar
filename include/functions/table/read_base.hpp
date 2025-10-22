#pragma once

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
#include <graphar/reader_util.h>

#include <filesystem>
#include <iostream>
#include <sstream>
#include <variant>

namespace duckdb {

using Reader = std::variant<graphar::VertexPropertyArrowChunkReader, graphar::AdjListArrowChunkReader,
                            graphar::AdjListPropertyArrowChunkReader>;

static graphar::Status next_chunk(Reader& reader) {
    return std::visit([](auto& r) { return r.next_chunk(); }, reader);
}

static graphar::Result<std::shared_ptr<arrow::Table>> GetChunk(Reader& reader) {
    DUCKDB_GRAPHAR_LOG_TRACE("GetChunk");
    return std::visit([](auto& r) { return r.GetChunk(); }, reader);
}

static graphar::Status seek_chunk_index(Reader& reader, graphar::IdType vertex_chunk_index) {
    return std::visit(
        [&](auto& r) {
            if constexpr (requires { r.seek_chunk_index(vertex_chunk_index); }) {
                return r.seek_chunk_index(vertex_chunk_index);
            } else {
                return graphar::Status::TypeError("seek_chunk_index is not implemented for this type of reader");
            }
        },
        reader);
}

static graphar::Status seek_vid(Reader& reader, graphar::IdType vid, const std::string& filter_column) {
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

static void Filter(Reader& reader, graphar::util::Filter filter) {
    return std::visit(
        [&](auto& r) {
            if constexpr (requires { r.Filter(filter); }) {
                r.Filter(filter);
            } else {
                return;
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
    idx_t columns_to_remove = 0;

    std::pair<graphar::IdType, graphar::IdType> vid_range = {-1, -1};
    std::string filter_column;

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
    vector<int> first_chunk_flags;
    vector<std::shared_ptr<arrow::Table>> tables;
    vector<idx_t> indices;
    vector<idx_t> sizes;
    std::pair<int64_t, int64_t> filter_range = {-1, -1};
    std::string function_name;
    int64_t total_rows = 0;
    vector<column_t> column_ids;
    idx_t columns_to_remove = 0;

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
        if (std::filesystem::path(graph_info->GetPrefix()).is_relative()) {
            throw IOException(
                "Using relative path as prefix is not supported. Please use absolute path or just remove this field.");
        }
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

    static graphar::Result<std::shared_ptr<arrow::Table>> NextChunk(idx_t reader_i,
                                                                    ReadBaseGlobalTableFunctionState& gstate) {
        DUCKDB_GRAPHAR_LOG_TRACE("ReadBase::NextChunk");
        auto& reader = gstate.readers[reader_i];
        int& first_chunk_flag = gstate.first_chunk_flags[reader_i];
        if (first_chunk_flag) {
            first_chunk_flag = false;
        } else {
            auto is_next = next_chunk(*reader);
            if (not is_next.ok()) {
                DUCKDB_GRAPHAR_LOG_DEBUG("No next chunk");
                return GraphArFunctions::EmptyTableFromNamesAndTypes(gstate.prop_names[reader_i],
                                                                     gstate.prop_types[reader_i]);
            }
        }
        auto result = GetChunk(*reader);
        if (result.has_error()) {
            throw std::runtime_error("Failed to get chunk" + result.status().message());
        }
        auto table = result.value();
        if (gstate.filter_range.first != -1) {
            if (gstate.total_rows >= gstate.filter_range.second) {
                DUCKDB_GRAPHAR_LOG_DEBUG("All rows read");
                return GraphArFunctions::EmptyTableFromNamesAndTypes(gstate.prop_names[reader_i],
                                                                     gstate.prop_types[reader_i]);
            } else if (gstate.total_rows + table->num_rows() < gstate.filter_range.first) {
                return NextChunk(reader_i, gstate);
            } else {
                auto start = std::max(static_cast<int64_t>(0), gstate.filter_range.first - gstate.total_rows);
                auto end =
                    std::min(table->num_rows(), static_cast<int64_t>(gstate.filter_range.second - gstate.total_rows));
                table = table->Slice(start, end - start);
            }
        }
        return table;
    }

    static std::shared_ptr<Reader> GetReader(ReadBaseGlobalTableFunctionState& gstate, ReadBindData& bind_data,
                                             idx_t ind, const std::string& filter_column) {
        return ReadFinal::GetReader(gstate, bind_data, ind, filter_column);
    }

    static void SetFilter(ReadBaseGlobalTableFunctionState& gstate, ReadBindData& bind_data,
                          const std::pair<graphar::IdType, graphar::IdType>& vid_range,
                          const std::string& filter_column) {
        ReadFinal::SetFilter(gstate, bind_data, vid_range, filter_column);
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
        gstate.columns_to_remove = bind_data.columns_to_remove;
        gstate.pgs = bind_data.pgs;
        gstate.column_ids = input.column_ids;
        if (gstate.column_ids.empty() ||
            (gstate.column_ids.size() == 1 && gstate.column_ids[0] == COLUMN_IDENTIFIER_ROW_ID)) {
            gstate.column_ids = {0};
        }
        gstate.readers.resize(bind_data.prop_types.size());
        gstate.first_chunk_flags.resize(gstate.readers.size(), true);
        gstate.tables.resize(gstate.readers.size());
        gstate.sizes.resize(gstate.readers.size());
        gstate.indices.resize(gstate.readers.size(), 0);

        DUCKDB_GRAPHAR_LOG_DEBUG("readers num: " + std::to_string(gstate.readers.size()));

        const auto& filter_column = bind_data.filter_column;

        idx_t reader_i = 0;
        std::generate(gstate.readers.begin(), gstate.readers.end(),
                      [&]() { return GetReader(gstate, bind_data, reader_i++, filter_column); });
        if (time_logging) {
            t.print("readers creation");
        }
        if (filter_column != "") {
            auto vid_range = bind_data.vid_range;
            const auto vertex_num = (filter_column == DST_GID_COLUMN)
                                        ? GraphArFunctions::GetVertexNum(bind_data.graph_info, bind_data.params[2])
                                        : GraphArFunctions::GetVertexNum(bind_data.graph_info, bind_data.params[0]);
            graphar::IdType zero = 0;
            vid_range.first = std::max(zero, vid_range.first);
            vid_range.second = std::min(vertex_num - 1, vid_range.second);
            if (vid_range.first > vid_range.second) {
                throw IOException("Invalid filter range");
            }
            SetFilter(gstate, bind_data, vid_range, filter_column);
        }
        if (time_logging) {
            t.print("filter setting");
        }

        gstate.prop_names = bind_data.prop_names;
        gstate.prop_types = bind_data.prop_types;

        for (idx_t i = 0; i < gstate.readers.size(); i++) {
            DUCKDB_GRAPHAR_LOG_TRACE("Get chunk for reader " + std::to_string(i));
            auto result = NextChunk(i, gstate);
            if (time_logging) {
                t.print("get_chunk");
            }
            if (result.has_error()) {
                throw std::runtime_error("Error while getting chunk: " + result.status().message());
            }
            gstate.tables[i] = result.value();
            if (i) {
                for (idx_t j = 0; j < bind_data.columns_to_remove; j++) {
                    gstate.tables[i] = gstate.tables[i]->RemoveColumn(0).ValueOrDie();
                }
            }
            DUCKDB_GRAPHAR_LOG_DEBUG("Table Schema: " + gstate.tables[i]->schema()->ToString());

            gstate.sizes[i] = gstate.tables[i]->num_rows();
            gstate.total_props_num += gstate.tables[i]->num_columns();
        }
        DUCKDB_GRAPHAR_LOG_DEBUG("total props num: " + std::to_string(gstate.total_props_num));

        if (time_logging) {
            t.print("additional info");
        }

        DUCKDB_GRAPHAR_LOG_DEBUG("::Init\n Done");
        if (time_logging) {
            t.print();
        }

        return make_uniq<ReadBaseGlobalTableFunctionState>(std::move(gstate));
    }

    static arrow::Result<std::shared_ptr<arrow::Table>> ConcatenateTables(
        const vector<std::shared_ptr<arrow::Table>>& tables) {
        DUCKDB_GRAPHAR_LOG_TRACE("ConcatenateTables started");
        if (tables.empty()) {
            return arrow::Status::Invalid("Cannot concatenate empty vector of tables");
        }

        const idx_t num_rows = tables[0]->num_rows();
        idx_t total_columns = 0;
        for (idx_t i = 1; i < tables.size(); ++i) {
            if (tables[i]->num_rows() != num_rows) {
                return arrow::Status::Invalid("All tables must have the same number of rows");
            }
            total_columns += tables[i]->num_columns();
        }

        vector<std::shared_ptr<arrow::Field>> all_fields;
        all_fields.reserve(total_columns);
        vector<std::shared_ptr<arrow::ChunkedArray>> all_columns;
        all_columns.reserve(total_columns);

        for (const auto& table : tables) {
            for (idx_t i = 0; i < table->num_columns(); ++i) {
                all_fields.emplace_back(table->field(i));
                all_columns.emplace_back(table->column(i));
            }
        }

        const auto combined_schema = std::make_shared<arrow::Schema>(std::move(all_fields));
        return arrow::Table::Make(std::move(combined_schema), std::move(all_columns), num_rows);
    }

    static void ConvertArrowTableToDataChunk(const arrow::Table& table, DataChunk& output,
                                             const vector<column_t>& column_ids, ClientContext& context) {
        auto schema = table.schema();

        ArrowSchema c_schema;
        auto export_schema_status = arrow::ExportSchema(*schema, &c_schema);
        if (!export_schema_status.ok()) {
            throw std::runtime_error("Failed to export schema: " + export_schema_status.message());
        }

        ArrowTableSchema arrow_table_schema;
        ArrowTableFunction::PopulateArrowTableSchema(context.db->config, arrow_table_schema, c_schema);

        const auto num_rows = table.num_rows();
        output.SetCapacity(num_rows);
        output.SetCardinality(num_rows);
        for (idx_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
            auto& arrow_type = *arrow_table_schema.GetColumns().at(column_ids[col_idx]);
            if (arrow_type.GetDuckType().id() == LogicalTypeId::VARCHAR) {
                for (idx_t row_i = 0; row_i < num_rows; row_i++) {
                    auto maybe_value = table.column(column_ids[col_idx])->GetScalar(row_i);
                    if (!maybe_value.ok()) {
                        throw std::runtime_error("Failed to get value from table: " + maybe_value.status().ToString());
                    }
                    auto value = maybe_value.ValueUnsafe();
                    auto duckdb_value = GraphArFunctions::ArrowScalar2DuckValue(value);
                    output.SetValue(col_idx, row_i, duckdb_value);
                }
                continue;
            }

            auto arrow_column = table.column(column_ids[col_idx]);

            auto flatten_result = arrow::Concatenate(arrow_column->chunks());
            if (!flatten_result.ok()) {
                throw std::runtime_error("Failed to flatten Arrow column");
            }
            auto arrow_array = flatten_result.ValueOrDie();

            ArrowArray c_array;
            auto export_array_status = arrow::ExportArray(*arrow_array, &c_array);
            if (!export_array_status.ok()) {
                throw std::runtime_error("Failed to export Arrow array: " + export_array_status.message());
            }

            ArrowArrayScanState array_state(context);
            array_state.owned_data = make_shared_ptr<ArrowArrayWrapper>();
            array_state.owned_data->arrow_array = std::move(c_array);

            ArrowToDuckDBConversion::SetValidityMask(output.data[col_idx], array_state.owned_data->arrow_array, 0,
                                                     output.size(), 0, -1);

            ArrowToDuckDBConversion::ColumnArrowToDuckDB(output.data[col_idx], array_state.owned_data->arrow_array, 0,
                                                         array_state, output.size(), arrow_type);
        }
    }

    static void Execute(ClientContext& context, TableFunctionInput& input, DataChunk& output) {
        bool time_logging = GraphArSettings::is_time_logging(context);

        ScopedTimer t("Execute");

        DUCKDB_GRAPHAR_LOG_DEBUG("::Execute Cast state");

        ReadBaseGlobalTableFunctionState& gstate = input.global_state->Cast<ReadBaseGlobalTableFunctionState>();

        DUCKDB_GRAPHAR_LOG_DEBUG("Chunk " + std::to_string(gstate.chunk_count) + ": Begin iteration");

        idx_t num_rows = (gstate.filter_range.first != -1 &&
                          gstate.total_rows == (gstate.filter_range.second - gstate.filter_range.first))
                             ? static_cast<idx_t>(0)
                             : STANDARD_VECTOR_SIZE;
        for (idx_t i = 0; i < gstate.readers.size() && num_rows; i++) {
            if (gstate.indices[i] == gstate.sizes[i]) {
                auto result = NextChunk(i, gstate);
                if (result.has_error()) {
                    throw std::runtime_error("Error while getting chunk: " + result.status().message());
                }
                gstate.tables[i] = result.value();
                if (i) {
                    for (idx_t j = 0; j < gstate.columns_to_remove; j++) {
                        gstate.tables[i] = gstate.tables[i]->RemoveColumn(0).ValueOrDie();
                    }
                }
                gstate.sizes[i] = gstate.tables[i]->num_rows();
                gstate.indices[i] = 0;
            }
            num_rows = std::min(num_rows, gstate.sizes[i] - gstate.indices[i]);
        }
        DUCKDB_GRAPHAR_LOG_DEBUG("num rows final: " + std::to_string(num_rows));

        if (num_rows > 0) {
            vector<std::shared_ptr<arrow::Table>> tables_to_convert(gstate.tables.size());
            for (idx_t i = 0; i < gstate.tables.size(); i++) {
                tables_to_convert[i] = gstate.tables[i]->Slice(gstate.indices[i], num_rows);
            }
            auto maybe_table = ConcatenateTables(tables_to_convert);
            if (!maybe_table.ok()) {
                throw std::runtime_error("Failed to concatenate tables: " + maybe_table.status().ToString());
            }
            auto table = maybe_table.ValueOrDie();
            ConvertArrowTableToDataChunk(*table, output, gstate.column_ids, context);
            for (idx_t i = 0; i < gstate.tables.size(); i++) {
                gstate.indices[i] += num_rows;
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
