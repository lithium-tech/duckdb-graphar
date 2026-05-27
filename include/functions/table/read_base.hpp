#pragma once

#include "readers/base_reader.hpp"
#include "readers/duck_arrow_chunk_reader.hpp"
#include "readers/duck_chunk_reader.hpp"
#include "utils/benchmark.hpp"
#include "utils/func.hpp"
#include "utils/global_log_manager.hpp"
#include "utils/type_info.hpp"

#include <arrow/c/bridge.h>

#include <duckdb/common/named_parameter_map.hpp>
#include <duckdb/function/table/arrow.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/planner/expression.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>

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

using BaseReaderPtr = std::variant<
    std::shared_ptr<graphar::TSVertexPropertyChunkInfoReader>, std::shared_ptr<graphar::TSAdjListChunkInfoReader>,
    std::shared_ptr<graphar::TSAdjListPropertyChunkInfoReader>,
    std::shared_ptr<graphar::TSVertexPropertyArrowChunkReader>, std::shared_ptr<graphar::TSAdjListArrowChunkReader>,
    std::shared_ptr<graphar::TSAdjListPropertyArrowChunkReader>>;

using ReaderPtr = std::variant<
    std::shared_ptr<graphar::DuckVertexPropertyArrowChunkReader>, std::shared_ptr<graphar::DuckAdjListArrowChunkReader>,
    std::shared_ptr<graphar::DuckAdjListPropertyArrowChunkReader>,
    std::shared_ptr<graphar::DuckVertexPropertyChunkReader>, std::shared_ptr<graphar::DuckAdjListChunkReader>,
    std::shared_ptr<graphar::DuckAdjListPropertyChunkReader>>;

template <typename SomeReader>
BaseReaderPtr ConvertBaseReader(graphar::Result<std::shared_ptr<SomeReader>> maybe_reader,
                                std::shared_ptr<graphar::SharedChunkCounter> counter = nullptr) {
    if (maybe_reader.has_error()) {
        throw InternalException("Error converting reader: " + maybe_reader.error().message());
    }
    return std::make_shared<graphar::ThreadSafeReader<SomeReader>>(std::move(maybe_reader.value()), counter);
}

template <typename SomeReader>
ReaderPtr ConvertReader(graphar::Result<std::shared_ptr<SomeReader>> maybe_reader) {
    if (maybe_reader.has_error()) {
        throw InternalException("Error converting reader: " + maybe_reader.error().message());
    }
    return maybe_reader.value();
}

static graphar::GetChunkFinalResult GetChunk(ReaderPtr& reader, int64_t num_rows) {
    DUCKDB_GRAPHAR_LOG_TRACE("GetChunk");
    return std::visit(
        [&num_rows](auto& r) -> graphar::GetChunkFinalResult {
            auto maybe_chunk = r->GetChunk(num_rows);
            if (maybe_chunk.has_error()) {
                throw InternalException("Error getting chunk: " + maybe_chunk.status().message());
            }
            return std::move(maybe_chunk.value());
        },
        reader);
}

static void FilterByRangeVertex(BaseReaderPtr& reader, std::pair<int64_t, int64_t> vid_range,
                                const std::string& filter_column, std::shared_ptr<graphar::VertexInfo> vertex_info) {
    return std::visit(
        [&](auto& r) {
            if constexpr (requires { r->FilterByRangeVertex(vid_range, filter_column, vertex_info); }) {
                r->FilterByRangeVertex(vid_range, filter_column, vertex_info);
            } else {
                throw InternalException("FilterByRangeVertex not implemented for this reader");
            }
        },
        reader);
}

static void FilterByRangeEdge(BaseReaderPtr& reader, const std::pair<int64_t, int64_t>& vid_range,
                              const std::string& filter_column, std::shared_ptr<graphar::EdgeInfo> edge_info,
                              const std::string& prefix) {
    return std::visit(
        [&](auto& r) {
            if constexpr (requires { r->FilterByRangeEdge(vid_range, filter_column, edge_info, prefix); }) {
                r->FilterByRangeEdge(vid_range, filter_column, edge_info, prefix);
            } else {
                throw InternalException("FilterByRangeEdge not implemented for this reader");
            }
        },
        reader);
}

static bool IsNullPtr(BaseReaderPtr& reader) {
    return std::visit([&](auto& r) { return (r == nullptr); }, reader);
}

static bool IsNullPtr(ReaderPtr& reader) {
    return std::visit([&](auto& r) { return (r == nullptr); }, reader);
}

static bool CheckIfNewFileNeeded(ReaderPtr& reader) {
    return std::visit([&](auto& r) { return r->CheckIfNewFileNeeded(); }, reader);
}
static void AcquirePathUnderLock(ReaderPtr& reader) {
    std::visit([&](auto& r) { r->AcquirePathUnderLock(); }, reader);
}
static idx_t GetRowsNum(ReaderPtr& reader) {
    return std::visit([&](auto& r) { return r->GetRowsNum(); }, reader);
}

static void SelectColumns(ReaderPtr& reader, std::vector<column_t> proj_columns) {
    return std::visit([&](auto& r) { r->SelectColumns(proj_columns); }, reader);
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

    vector<std::pair<graphar::IdType, graphar::IdType>> vid_ranges;
    std::string filter_column;

    TypeInfoPtr type_info;

    template <typename ReadFinal>
    friend class ReadBase;
    friend class ReadVertices;
    friend class ReadEdges;
};

class ReadBaseGlobalTableFunctionState : public GlobalTableFunctionState {
public:
    idx_t MaxThreads() const override { return MAX_THREADS; }
    std::mutex lock;

private:
    vector<std::string> params;
    graphar::PropertyGroupVector pgs;
    vector<vector<std::string>> prop_names;
    vector<vector<std::string>> prop_types;
    std::atomic<idx_t> chunk_count = 0;
    idx_t total_props_num = 0;
    vector<vector<BaseReaderPtr>> base_readers;

    TypeInfoPtr type_info;
    std::shared_ptr<graphar::GraphInfo> graph_info;

    std::pair<int64_t, int64_t> filter_range = {-1, -1};
    std::string filter_column;
    std::string function_name;
    std::atomic<int64_t> total_rows = 0;
    vector<column_t> column_ids;
    vector<vector<column_t>> global_projected_inds;
    vector<vector<column_t>> local_projected_inds;
    idx_t id_columns_num = 0;

    template <typename ReadFinal>
    friend class ReadBase;
    friend class ReadVertices;
    friend class ReadEdges;
};

class ReadBaseLocalTableFunctionState : public LocalTableFunctionState {
private:
    vector<ReaderPtr> readers;
    std::shared_ptr<DuckParquetFileReader> file_reader;
    vector<unique_ptr<DataChunk>> cur_chunks;
    idx_t cur_chunk_id = 0;

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
        const auto& prefix = graph_info->GetPrefix();
        if (!prefix.starts_with("s3://") && std::filesystem::path(prefix).is_relative()) {
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

    static BaseReaderPtr GetBaseReader(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate, idx_t ind,
                                       const std::string& filter_column,
                                       std::shared_ptr<graphar::SharedChunkCounter> counter = nullptr) {
        return ReadFinal::GetBaseReader(context, gstate, ind, filter_column, counter);
    }

    static void SetFilter(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate, idx_t ind,
                          const vector<std::pair<int64_t, int64_t>>& vid_ranges, const std::string& filter_column) {
        return ReadFinal::SetFilter(context, gstate, ind, vid_ranges, filter_column);
    }

    static ReaderPtr GetReader(ClientContext& context, ReadBaseGlobalTableFunctionState& gstate,
                               ReadBaseLocalTableFunctionState& lstate, idx_t ind, const std::string& filter_column) {
        return ReadFinal::GetReader(context, gstate, lstate, ind, filter_column);
    }

    static const vector<std::pair<graphar::IdType, graphar::IdType>>& GetVidRanges(const ReadBindData& bind_data) {
        return bind_data.vid_ranges;
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

        auto gstate_ptr = make_uniq<ReadBaseGlobalTableFunctionState>();
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

        auto& vid_ranges = bind_data.vid_ranges;
        const bool has_filter = filter_column != "";
        const idx_t num_ranges = has_filter ? vid_ranges.size() : 1;

        if (gstate.column_ids.empty() ||
            gstate.column_ids.size() == 1 && gstate.column_ids[0] == COLUMN_IDENTIFIER_ROW_ID) {
            DUCKDB_GRAPHAR_LOG_DEBUG("Returning any column");
            local_projected_inds[0].emplace_back(0);
            gstate.base_readers[0].resize(num_ranges);
            auto counter = has_filter ? std::make_shared<graphar::SharedChunkCounter>() : nullptr;
            for (idx_t r = 0; r < num_ranges; ++r) {
                gstate.base_readers[0][r] = GetBaseReader(context, gstate, 0, filter_column, counter);
            }
            gstate.global_projected_inds[0].emplace_back(0);
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
                gstate.base_readers[i].resize(num_ranges);
                auto counter = has_filter ? std::make_shared<graphar::SharedChunkCounter>() : nullptr;
                for (idx_t r = 0; r < num_ranges; ++r) {
                    gstate.base_readers[i][r] = GetBaseReader(context, gstate, i, filter_column, counter);
                }
            }
        }

        DUCKDB_GRAPHAR_LOG_DEBUG("readers num: " + std::to_string(gstate.base_readers.size()));

        if (has_filter) {
            DUCKDB_GRAPHAR_LOG_TRACE("Filters found");
            const auto vertex_num = GetCountClass::GetCount(gstate.type_info, bind_data.GetGraphInfo()->GetPrefix());
            graphar::IdType zero = 0;
            for (idx_t r = 0; r < num_ranges; ++r) {
                vid_ranges[r].first = std::max(zero, vid_ranges[r].first);
                vid_ranges[r].second = std::min(vertex_num, vid_ranges[r].second);
                if (vid_ranges[r].first >= vid_ranges[r].second) {
                    throw IOException("Invalid filter range: " + std::to_string(vid_ranges[r].first) + " > " +
                                      std::to_string(vid_ranges[r].second));
                }
            }
            for (idx_t ind = 0; ind < prop_types_size; ++ind) {
                if (gstate.base_readers[ind].empty() || IsNullPtr(gstate.base_readers[ind][0])) {
                    continue;
                }
                SetFilter(context, gstate, ind, vid_ranges, filter_column);
            }
        }
        if (time_logging) {
            t.print("filter setting");
        }

        gstate.local_projected_inds = std::move(local_projected_inds);

        DUCKDB_GRAPHAR_LOG_DEBUG("::Init Done");
        if (time_logging) {
            t.print();
        }

        return gstate_ptr;
    }

    static unique_ptr<LocalTableFunctionState> InitLocal(ExecutionContext& context, TableFunctionInitInput& input,
                                                         GlobalTableFunctionState* gstate_ptr) {
        DUCKDB_GRAPHAR_LOG_TRACE("Local init started");
        auto bind_data = input.bind_data->Cast<ReadBindData>();

        DUCKDB_GRAPHAR_LOG_TRACE(bind_data.function_name + "::Init");

        auto lstate_ptr = make_uniq<ReadBaseLocalTableFunctionState>();
        auto& lstate = *lstate_ptr;
        auto& gstate = gstate_ptr->Cast<ReadBaseGlobalTableFunctionState>();

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

        return lstate_ptr;
    }

    static void Execute(ClientContext& context, TableFunctionInput& input, DataChunk& output) {
        bool time_logging = GraphArSettings::is_time_logging(context);

        ScopedTimer t("Execute");

        DUCKDB_GRAPHAR_LOG_DEBUG("::Execute Cast state");

        ReadBaseGlobalTableFunctionState& gstate = input.global_state->Cast<ReadBaseGlobalTableFunctionState>();
        ReadBaseLocalTableFunctionState& lstate = input.local_state->Cast<ReadBaseLocalTableFunctionState>();

        DUCKDB_GRAPHAR_LOG_DEBUG("Chunk " + std::to_string(gstate.chunk_count) + ": Begin iteration");

        bool needs_new_file = false;
        for (auto& reader : lstate.readers) {
            if (IsNullPtr(reader)) continue;
            if (CheckIfNewFileNeeded(reader)) {
                needs_new_file = true;
            }
        }

        if (needs_new_file) {
            std::lock_guard<std::mutex> guard(gstate.lock);
            for (auto& reader : lstate.readers) {
                if (IsNullPtr(reader)) continue;
                AcquirePathUnderLock(reader);
            }
        }

        idx_t num_rows = STANDARD_VECTOR_SIZE;
        for (auto& reader : lstate.readers) {
            if (IsNullPtr(reader)) continue;
            num_rows = std::min(num_rows, GetRowsNum(reader));
        }
        DUCKDB_GRAPHAR_LOG_DEBUG("num rows final: " + std::to_string(num_rows));

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
                    output.data[gstate.global_projected_inds[i][j]].Reference(lstate.cur_chunks[i]->data[j]);
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

    template <typename ValidateFunc>
    static void PushdownComplexFilterImpl(ClientContext& context, ReadBindData& read_bind_data,
                                          vector<unique_ptr<Expression>>& filters, ValidateFunc validate_and_extract) {
        if (!read_bind_data.vid_ranges.empty()) {
            return;
        }
        std::set<int64_t> vids;
        vector<unique_ptr<Expression>> filters_new;
        bool already_pushed = false;

        // Wrapper lambda that captures vids by reference
        auto validate_wrapper = [&](const std::string& col, const Value& val) -> bool {
            if (validate_and_extract(col, val)) {
                if (!val.IsNull()) {
                    vids.insert(val.GetValue<int64_t>());
                }
                return true;
            }
            return false;
        };

        for (auto& filter : filters) {
            if (already_pushed) {
                filters_new.push_back(std::move(filter));
                continue;
            }

            bool can_pushdown = false;

            // Case 0: equality comparison (col = value)  
            if (filter->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {  
                auto& comparison = filter->Cast<BoundComparisonExpression>();  
                if (comparison.GetExpressionType() == ExpressionType::COMPARE_EQUAL) {  
                    bool left_is_scalar = comparison.left->IsFoldable();  
                    bool right_is_scalar = comparison.right->IsFoldable();  
                    if (left_is_scalar || right_is_scalar) {  
                        auto column_name = comparison.left->ToString();  
                        Value val;  
                        
                        auto& scalar_expr = (comparison.left->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF)   
                                        ? *comparison.right   
                                        : *comparison.left;  
                        
                        if (!ExpressionExecutor::TryEvaluateScalar(context, scalar_expr, val)) {
                            continue;
                        }  
                        
                        if (validate_wrapper(column_name, val)) {  
                            can_pushdown = true;  
                            read_bind_data.filter_column = column_name;  
                        }  
                    }  
                }  
            }

            // Case 1: list_contains([1, 2, 3], col) -- list literal
            if (!can_pushdown && filter->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
                auto& op_expr = filter->Cast<BoundFunctionExpression>();
                const auto& fname = op_expr.function.name;
                if (fname == "contains" || fname == "list_contains" || fname == "array_contains" ||
                    fname == "list_has" || fname == "array_has") {
                    if (op_expr.children.size() == 2 &&
                        op_expr.children[0]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
                        auto& const_expr = op_expr.children[0]->Cast<BoundConstantExpression>();
                        auto column_name = op_expr.children[1]->ToString();
                        auto& list_value = const_expr.value;

                        if (list_value.type().id() == LogicalTypeId::LIST) {
                            auto list_children = ListValue::GetChildren(list_value);
                            bool any = false;
                            for (const auto& child : list_children) {
                                if (validate_wrapper(column_name, child)) any = true;
                            }
                            if (any) {
                                read_bind_data.filter_column = column_name;
                                can_pushdown = true;
                            }
                        }
                    }
                }
            }

            // Case 2: col IN (1, 2, 3) -- SQL IN operator
            if (!can_pushdown && filter->GetExpressionClass() == ExpressionClass::BOUND_OPERATOR &&
                filter->GetExpressionType() == ExpressionType::COMPARE_IN) {
                auto& op_expr = filter->Cast<BoundOperatorExpression>();
                if (op_expr.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
                    auto column_name = op_expr.children[0]->ToString();
                    bool any = false;
                    for (idx_t i = 1; i < op_expr.children.size(); i++) {
                        if (op_expr.children[i]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
                            auto& cv = op_expr.children[i]->Cast<BoundConstantExpression>().value;
                            if (validate_wrapper(column_name, cv)) any = true;
                        }
                    }
                    if (any) {
                        read_bind_data.filter_column = column_name;
                        can_pushdown = true;
                    }
                }
            }

            // Case 3: col IN (1, 2, 3) after InClauseRewriter
            // Small lists get rewritten to: col=1 OR col=2 OR col=3
            if (!can_pushdown && filter->GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION &&
                filter->GetExpressionType() == ExpressionType::CONJUNCTION_OR) {
                auto& conj = filter->Cast<BoundConjunctionExpression>();
                std::string column_name;
                std::vector<Value> local_vals;
                bool valid = true;

                for (auto& child : conj.children) {
                    if (child->GetExpressionClass() != ExpressionClass::BOUND_COMPARISON ||
                        child->GetExpressionType() != ExpressionType::COMPARE_EQUAL) {
                        valid = false;
                        break;
                    }
                    auto& cmp = child->Cast<BoundComparisonExpression>();
                    std::string col;
                    Value val;
                    if (cmp.left->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
                        cmp.right->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
                        col = cmp.left->ToString();
                        val = cmp.right->Cast<BoundConstantExpression>().value;
                    } else if (cmp.right->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
                               cmp.left->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
                        col = cmp.right->ToString();
                        val = cmp.left->Cast<BoundConstantExpression>().value;
                    } else {
                        valid = false;
                        break;
                    }

                    if (column_name.empty())
                        column_name = col;
                    else if (column_name != col) {
                        valid = false;
                        break;
                    }
                    local_vals.push_back(val);
                }

                if (valid && !column_name.empty()) {
                    bool any = false;
                    for (auto& v : local_vals)
                        if (validate_wrapper(column_name, v)) any = true;
                    if (any) {
                        read_bind_data.filter_column = column_name;
                        can_pushdown = true;
                    }
                }
            }

            if (!can_pushdown) {
                filters_new.push_back(std::move(filter));
            } else {
                already_pushed = true;
            }
        }

        if (already_pushed) {
            int64_t vertex_num = 0;
            const auto& graph_info = read_bind_data.GetGraphInfo();
            if (read_bind_data.filter_column == GID_COLUMN_INTERNAL) {
                auto vertex_info = *std::get_if<std::shared_ptr<graphar::VertexInfo>>(&read_bind_data.type_info);
                vertex_num = GetCountClass::GetCount(read_bind_data.type_info, graph_info->GetPrefix());
            } else {
                auto edge_info = *std::get_if<std::shared_ptr<graphar::EdgeInfo>>(&read_bind_data.type_info);
                vertex_num = (read_bind_data.filter_column == SRC_GID_COLUMN)
                                 ? GetCountClass::GetCount(graph_info->GetVertexInfo(edge_info->GetSrcType()),
                                                           graph_info->GetPrefix())
                                 : GetCountClass::GetCount(graph_info->GetVertexInfo(edge_info->GetDstType()),
                                                           graph_info->GetPrefix());
            }
            for (const auto& vid : vids) {
                if (0 <= vid && vid < vertex_num) {
                    read_bind_data.vid_ranges.push_back({vid, vid + 1});
                }
            }
        }

        filters = std::move(filters_new);
    }

    static void Register(ExtensionLoader& loader) { loader.RegisterFunction(ReadFinal::GetFunction()); }
    static TableFunction GetFunction() { return ReadFinal::GetFunction(); }
    static TableFunction GetScanFunction() { return ReadFinal::GetScanFunction(); }
    static OperatorPartitionData GetPartitionData(ClientContext& context, TableFunctionGetPartitionInput& input) {
        auto& lstate = input.local_state->Cast<ReadBaseLocalTableFunctionState>();
        return OperatorPartitionData(lstate.cur_chunk_id);
    }
};
}  // namespace duckdb
