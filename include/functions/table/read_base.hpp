#pragma once

#include <cassert>
#include <iostream>
#include <variant>

#include "arrow/c/bridge.h"
#include "duckdb/common/named_parameter_map.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "graphar/api/arrow_reader.h"
#include "graphar/api/high_level_reader.h"
#include "graphar/arrow/chunk_reader.h"
#include "graphar/expression.h"
#include "graphar/fwd.h"
#include "graphar/reader_util.h"
#include "utils/benchmark.hpp"
#include "utils/func.hpp"
#include "utils/global_log_manager.hpp"

namespace duckdb {

using Reader = std::variant<graphar::VertexPropertyArrowChunkReader, graphar::AdjListArrowChunkReader,
                            graphar::AdjListPropertyArrowChunkReader>;

static graphar::Status next_chunk(Reader& reader) {
    return std::visit([](auto& r) { return r.next_chunk(); }, reader);
}

static graphar::Result<std::shared_ptr<arrow::Table>> GetChunk(Reader& reader) {
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

private:
    std::vector<std::vector<std::string>> prop_names;
    std::vector<std::string> flatten_prop_names;
    std::vector<std::vector<std::string>> prop_types;
    std::vector<std::string> flatten_prop_types;
    std::shared_ptr<graphar::GraphInfo> graph_info;
    std::string function_name;
    std::vector<std::string> params;
    graphar::PropertyGroupVector pgs;
    idx_t columns_to_remove = 0;

    template <typename ReadFinal>
    friend class ReadBase;
    friend class ReadVertices;
    friend class ReadEdges;
};

class ReadBaseGlobalTableFunctionState : public GlobalTableFunctionState {
private:
    graphar::PropertyGroupVector pgs;
    std::vector<std::vector<std::string>> prop_names;
    std::vector<std::vector<std::string>> prop_types;
    std::vector<vector<LogicalType>> prop_types_duck;
    idx_t chunk_count = 0;
    idx_t total_props_num = 0;
    std::vector<std::shared_ptr<Reader>> readers;
    std::vector<std::vector<std::shared_ptr<ArrowArray>>> ptrs;
    std::vector<int> first_chunk;
    std::vector<std::shared_ptr<arrow::Table>> tables;
    std::vector<std::vector<idx_t>> indices;
    std::vector<std::vector<idx_t>> chunk_ids;
    std::vector<std::vector<idx_t>> sizes;
    arrow_column_map_t arrow_convert_data;
    std::pair<int64_t, int64_t> filter_range = {-1, -1};
    std::string function_name;
    int64_t total_rows = 0;

    template <typename ReadFinal>
    friend class ReadBase;
    friend class ReadVertices;
    friend class ReadEdges;
};

template <typename ReadFinal>
class ReadBase {
public:
    static unique_ptr<FunctionData> Bind(ClientContext& context, TableFunctionBindInput& input,
                                         vector<LogicalType>& return_types, vector<string>& names) {
        return ReadFinal::Bind(context, input, return_types, names);
    }

    static graphar::Result<std::shared_ptr<arrow::Table>> NextChunk(std::shared_ptr<Reader> reader, int& is_first,
                                                                    ReadBaseGlobalTableFunctionState& gstate) {
        if (is_first) {
            is_first = false;
        } else {
            auto is_next = next_chunk(*reader);
            if (not is_next.ok()) {
                return is_next;
            }
        }
        auto result = GetChunk(*reader);
        assert(!result.has_error());
        auto table = result.value();
        if (gstate.filter_range.first != -1) {
            if (gstate.total_rows >= gstate.filter_range.second) {
                return graphar::Status::IndexError("No more filtered data");
            } else if (gstate.total_rows + table->num_rows() < gstate.filter_range.first) {
                return NextChunk(reader, is_first, gstate);
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
                                             idx_t ind, const std::string& filter_value,
                                             const std::string& filter_column, const std::string& filter_type) {
        return ReadFinal::GetReader(gstate, bind_data, ind, filter_value, filter_column, filter_type);
    }

    static void SetFilter(ReadBaseGlobalTableFunctionState& gstate, ReadBindData& bind_data, std::string& filter_value,
                          std::string& filter_column, std::string& filter_type) {
        ReadFinal::SetFilter(gstate, bind_data, filter_value, filter_column, filter_type);
    }

    static unique_ptr<GlobalTableFunctionState> Init(ClientContext& context, TableFunctionInitInput& input) {
        bool time_logging = GraphArSettings::is_time_logging(context);

        ScopedTimer t("StateInit");

        LOG_TRACE("::Init\n Cast bind data");

        auto bind_data = input.bind_data->Cast<ReadBindData>();

        if (time_logging) {
            t.print("cast");
        }

        ReadBaseGlobalTableFunctionState gstate;

        gstate.function_name = bind_data.function_name;
        gstate.pgs = bind_data.pgs;
        gstate.readers.resize(bind_data.prop_types.size());
        gstate.first_chunk.resize(gstate.readers.size(), true);
        gstate.tables.resize(gstate.readers.size());
        gstate.sizes.resize(gstate.readers.size());
        gstate.indices.resize(gstate.readers.size());
        gstate.chunk_ids.resize(gstate.readers.size());
        gstate.prop_types_duck.resize(gstate.readers.size());
        gstate.ptrs.resize(gstate.readers.size());

        LOG_DEBUG("readers num: " + std::to_string(gstate.readers.size()));

        std::string filter_value, filter_column, filter_type;
        if (input.filters) {
            LOG_DEBUG("Found filters");

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
            LOG_DEBUG("filter column: " + filter_column + " filter type: " + filter_type +
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

        for (idx_t i = 0; i < gstate.readers.size(); i++) {
            auto result = NextChunk(gstate.readers[i], gstate.first_chunk[i], gstate);
            if (time_logging) {
                t.print("get_chunk");
            }
            assert(!result.has_error());
            gstate.tables[i] = result.value();
            if (i) {
                for (idx_t j = 0; j < bind_data.columns_to_remove; j++) {
                    gstate.tables[i] = gstate.tables[i]->RemoveColumn(0).ValueOrDie();
                }
            }
            LOG_DEBUG("Table Schema: " + gstate.tables[i]->schema()->ToString());

            gstate.sizes[i].resize(gstate.tables[i]->num_columns());
            gstate.indices[i].resize(gstate.sizes[i].size(), 0);
            gstate.chunk_ids[i].resize(gstate.sizes[i].size(), 0);
            gstate.prop_types_duck[i].resize(gstate.sizes[i].size());
            gstate.ptrs[i].resize(gstate.sizes[i].size());
            for (idx_t j = 0; j < gstate.sizes[i].size(); j++) {
                gstate.sizes[i][j] = gstate.tables[i]->column(j)->chunk(0)->length();
                gstate.prop_types_duck[i][j] = GraphArFunctions::graphArT2duckT(bind_data.prop_types[i][j]);
                gstate.arrow_convert_data[gstate.total_props_num + j] = make_shared_ptr<ArrowType>(
                    gstate.prop_types_duck[i][j],
                    std::move(GraphArFunctions::graphArT2ArrowTypeInfo(bind_data.prop_types[i][j])));
            }
            gstate.total_props_num += gstate.tables[i]->num_columns();
        }
        LOG_DEBUG("total props num: " + std::to_string(gstate.total_props_num));

        gstate.prop_names = bind_data.prop_names;
        gstate.prop_types = bind_data.prop_types;

        if (time_logging) {
            t.print("additional info");
        }

        LOG_DEBUG("::Init\n Done");
        if (time_logging) {
            t.print();
        }

        return make_uniq<ReadBaseGlobalTableFunctionState>(std::move(gstate));
    }

    static void Execute(ClientContext& context, TableFunctionInput& input, DataChunk& output) {
        bool time_logging = GraphArSettings::is_time_logging(context);

        ScopedTimer t("Execute");

        LOG_DEBUG("::Execute\n Cast state");

        ReadBaseGlobalTableFunctionState& gstate = input.global_state->Cast<ReadBaseGlobalTableFunctionState>();

        LOG_DEBUG("Chunk " + std::to_string(gstate.chunk_count) + ": Begin iteration");

        idx_t num_rows = STANDARD_VECTOR_SIZE;
        for (idx_t i = 0; i < gstate.readers.size(); i++) {
            if (gstate.indices[i][0] == gstate.sizes[i][0] and
                gstate.chunk_ids[i][0] + 1 == gstate.tables[i]->column(0)->num_chunks()) {
                if (not next_chunk(*gstate.readers[i]).ok()) {
                    num_rows = 0;
                    break;
                }
            }
        }
        if (num_rows > 0) {
            for (idx_t i = 0; i < gstate.readers.size(); i++) {
                for (int prop_i = 0; prop_i < gstate.prop_names[i].size(); ++prop_i) {
                    if (gstate.indices[i][prop_i] == gstate.sizes[i][prop_i]) {
                        gstate.chunk_ids[i][prop_i]++;
                        if (gstate.tables[i]->column(prop_i)->num_chunks() == gstate.chunk_ids[i][prop_i]) {
                            auto result = NextChunk(gstate.readers[i], gstate.first_chunk[i], gstate);
                            assert(!result.has_error());
                            gstate.tables[i] = result.value();
                            for (int prop_ii = 0; prop_ii < gstate.prop_names[i].size(); ++prop_ii) {
                                gstate.chunk_ids[i][prop_ii] = 0;
                                gstate.sizes[i][prop_ii] =
                                    gstate.tables[i]->column(prop_ii)->chunk(gstate.chunk_ids[i][prop_ii])->length();
                                gstate.indices[i][prop_ii] = 0;
                            }
                        } else {
                            gstate.sizes[i][prop_i] =
                                gstate.tables[i]->column(prop_i)->chunk(gstate.chunk_ids[i][prop_i])->length();
                            gstate.indices[i][prop_i] = 0;
                        }
                    }
                }
                for (int prop_i = 0; prop_i < gstate.prop_names[i].size(); ++prop_i) {
                    num_rows = std::min(num_rows, gstate.sizes[i][prop_i] - gstate.indices[i][prop_i]);
                }
            }
            LOG_DEBUG("num rows final: " + std::to_string(num_rows));

            auto fake_wrapper = make_uniq<ArrowArrayWrapper>();
            fake_wrapper->arrow_array.length = num_rows;
            fake_wrapper->arrow_array.release = release_children_only;
            fake_wrapper->arrow_array.n_children = gstate.total_props_num;
            fake_wrapper->arrow_array.children =
                (class ArrowArray**)malloc(gstate.total_props_num * sizeof(class ArrowArray*));

            idx_t props_before = 0;
            for (idx_t i = 0; i < gstate.readers.size(); i++) {
                for (int prop_i = 0; prop_i < gstate.prop_names[i].size(); ++prop_i) {
                    gstate.ptrs[i][prop_i] = std::make_shared<ArrowArray>();
                    gstate.ptrs[i][prop_i]->release = release_children_only;
                    auto raw_arr_ptr = gstate.tables[i]
                                           ->column(prop_i)
                                           ->chunk(gstate.chunk_ids[i][prop_i])
                                           ->Slice(gstate.indices[i][prop_i], num_rows);
                    arrow::Status status = arrow::ExportArray(*raw_arr_ptr, gstate.ptrs[i][prop_i].get(), nullptr);
                    assert(status.ok());

                    fake_wrapper->arrow_array.children[props_before + prop_i] = gstate.ptrs[i][prop_i].get();
                }
                props_before += gstate.prop_names[i].size();
            }
            ArrowScanLocalState local_state(std::move(fake_wrapper), context);
            props_before = 0;
            for (idx_t i = 0; i < gstate.readers.size(); i++) {
                for (int prop_i = 0; prop_i < gstate.prop_names[i].size(); ++prop_i) {
                    auto wrapper = make_uniq<ArrowArrayWrapper>();

                    auto fake_local_state = make_uniq<ArrowScanLocalState>(std::move(wrapper), context);
                    auto ptrh = make_uniq<ArrowArrayScanState>(*fake_local_state, context);
                    local_state.array_states[props_before + prop_i] = std::move(ptrh);
                }
                props_before += gstate.prop_names[i].size();
            }
            local_state.chunk->arrow_array.children[0]->release = release_children_only;
            local_state.chunk->arrow_array.children[0]->length = num_rows;

            ArrowTableFunction::ArrowToDuckDB(local_state, gstate.arrow_convert_data, output, 0);

            for (idx_t i = 0; i < gstate.readers.size(); i++) {
                for (int prop_i = 0; prop_i < gstate.prop_names[i].size(); ++prop_i) {
                    gstate.indices[i][prop_i] += num_rows;
                }
            }
        }

        output.SetCapacity(num_rows);
        output.SetCardinality(num_rows);
        gstate.total_rows += num_rows;
        LOG_DEBUG("Size of chunk: " + std::to_string(num_rows) + " Total size: " + std::to_string(gstate.total_rows))
        if (time_logging) {
            t.print();
        }
        gstate.chunk_count++;
    }

    static void Register(DatabaseInstance& db) { ExtensionUtil::RegisterFunction(db, ReadFinal::GetFunction()); }
    static TableFunction GetFunction() { return ReadFinal::GetFunction(); }
};
}  // namespace duckdb
