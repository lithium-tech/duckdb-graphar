#include "functions/table/degree.hpp"

#include "utils/benchmark.hpp"
#include "utils/func.hpp"
#include "utils/global_log_manager.hpp"
#include "utils/pua_init.hpp"
#include "utils/type_info.hpp"

#include <duckdb/common/named_parameter_map.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>

#include <graphar/api/high_level_reader.h>
#include <graphar/graph_info.h>

#include <algorithm>
#include <set>

namespace duckdb {

//-------------------------------------------------------------------
// Bind
//-------------------------------------------------------------------
unique_ptr<FunctionData> Degree::Bind(ClientContext& context, TableFunctionBindInput& input,
                                      vector<LogicalType>& return_types, vector<Identifier>& names) {
    bool time_logging = GraphArSettings::is_time_logging(context);

    ScopedTimer t("Bind");

    DUCKDB_GRAPHAR_LOG_TRACE("Degree::Bind");

    const auto file_path = StringValue::Get(input.inputs[0]);
    const std::string src_type = StringValue::Get(input.named_parameters.at("src"));
    const std::string dst_type = StringValue::Get(input.named_parameters.at("dst"));
    const std::string e_type = StringValue::Get(input.named_parameters.at("type"));

    auto bind_data = make_uniq<DegreeBindData>();
    bind_data->file_path = file_path;

    auto maybe_graph_info = graphar::GraphInfo::Load(file_path);
    if (maybe_graph_info.has_error()) {
        throw IOException("Failed to load graph info from path: %s", file_path);
    }
    bind_data->graph_info = maybe_graph_info.value();

    auto edge_info = bind_data->graph_info->GetEdgeInfo(src_type, e_type, dst_type);
    if (!edge_info) {
        throw BinderException("Edges of this type are not found");
    }
    bind_data->edge_info = edge_info;

    bind_data->has_src = edge_info->HasAdjacentListType(graphar::AdjListType::ordered_by_source);
    bind_data->has_dst = edge_info->HasAdjacentListType(graphar::AdjListType::ordered_by_dest);

    const auto prefix = bind_data->graph_info->GetPrefix();
    auto src_vertex_info = bind_data->graph_info->GetVertexInfo(edge_info->GetSrcType());
    if (!src_vertex_info) {
        throw BinderException("Source vertex type not found");
    }
    bind_data->vertex_count = GetCountClass::GetCount(src_vertex_info, prefix);

    return_types.push_back(LogicalType::BIGINT);
    names.push_back(Identifier(GID_COLUMN_INTERNAL));
    return_types.push_back(LogicalType::BIGINT);
    names.push_back("out_degree");
    return_types.push_back(LogicalType::BIGINT);
    names.push_back("in_degree");

    usage_analytics::EmitGraphOperationEvent(context, "degree", bind_data->file_path);

    DUCKDB_GRAPHAR_LOG_DEBUG("Degree::Bind finish");
    if (time_logging) {
        t.print();
    }

    return bind_data;
}
//-------------------------------------------------------------------
// GetOffsetColumn
//-------------------------------------------------------------------
std::shared_ptr<arrow::ChunkedArray> Degree::GetOffsetColumn(DegreeGlobalState& state,
                                                             graphar::AdjListType adj_list_type, graphar::IdType vid) {
    graphar::IdType chunk_size;
    if (adj_list_type == graphar::AdjListType::ordered_by_source) {
        chunk_size = state.edge_info->GetSrcChunkSize();
    } else {
        chunk_size = state.edge_info->GetDstChunkSize();
    }
    graphar::IdType chunk_index = vid / chunk_size;

    DegreeOffsetCache& cache =
        (adj_list_type == graphar::AdjListType::ordered_by_source) ? state.src_cache : state.dst_cache;
    if (cache.chunk_index == chunk_index && cache.column) {
        return cache.column;
    }

    auto offset_file_path = state.edge_info->GetAdjListOffsetFilePath(chunk_index, adj_list_type).value();
    std::string out_prefix;
    auto fs = graphar::FileSystemFromUriOrPath(state.prefix, &out_prefix).value();
    auto adjacent_list = state.edge_info->GetAdjacentList(adj_list_type);
    auto file_type = adjacent_list->GetFileType();
    std::string path = out_prefix + offset_file_path;
    DUCKDB_GRAPHAR_LOG_DEBUG("Opening offset file: " + path);
    auto table = fs->ReadFileToTable(path, file_type).value();

    cache.chunk_index = chunk_index;
    cache.column = table->column(0);
    return cache.column;
}
//-------------------------------------------------------------------
// ReadDegrees
//-------------------------------------------------------------------
std::vector<int64_t> Degree::ReadDegrees(DegreeGlobalState& state, graphar::AdjListType adj_list_type,
                                         graphar::IdType start, graphar::IdType end) {
    if (start >= end) {
        return {};
    }
    graphar::IdType chunk_size;
    if (adj_list_type == graphar::AdjListType::ordered_by_source) {
        chunk_size = state.edge_info->GetSrcChunkSize();
    } else {
        chunk_size = state.edge_info->GetDstChunkSize();
    }

    std::vector<int64_t> result;
    result.reserve(end - start);

    graphar::IdType first_chunk = start / chunk_size;
    graphar::IdType last_chunk = (end - 1) / chunk_size;
    for (graphar::IdType c = first_chunk; c <= last_chunk; ++c) {
        auto column = GetOffsetColumn(state, adj_list_type, c * chunk_size);
        graphar::IdType local_start = start - c * chunk_size;
        if (local_start < 0) {
            local_start = 0;
        }
        graphar::IdType local_end = end - c * chunk_size;
        if (local_end > chunk_size) {
            local_end = chunk_size;
        }
        if (local_end <= local_start) {
            continue;
        }
        auto slice = column->Slice(local_start, (local_end - local_start) + 1);
        int64_t prev = 0;
        bool have_prev = false;
        for (const auto& chunk : slice->chunks()) {
            auto arr = std::static_pointer_cast<arrow::Int64Array>(chunk);
            const auto* raw = arr->raw_values();
            for (int64_t i = 0; i < arr->length(); ++i) {
                if (have_prev) {
                    result.push_back(raw[i] - prev);
                }
                prev = raw[i];
                have_prev = true;
            }
        }
    }
    return result;
}
//-------------------------------------------------------------------
// State Init
//-------------------------------------------------------------------
unique_ptr<GlobalTableFunctionState> DegreeGlobalTableFunctionState::Init(ClientContext& context,
                                                                          TableFunctionInitInput& input) {
    auto& bind_data = input.bind_data->Cast<DegreeBindData>();

    auto gstate = DegreeGlobalState();
    gstate.edge_info = bind_data.edge_info;
    gstate.prefix = bind_data.graph_info->GetPrefix();
    gstate.vertex_count = bind_data.vertex_count;
    gstate.has_src = bind_data.has_src;
    gstate.has_dst = bind_data.has_dst;

    // Build projection: output column i maps to logical column projection[i].
    if (input.column_ids.empty() || (input.column_ids.size() == 1 && input.column_ids[0] == COLUMN_IDENTIFIER_ROW_ID)) {
        gstate.projection.push_back(0);
    } else {
        for (const auto& col : input.column_ids) {
            gstate.projection.push_back(col);
        }
    }

    for (const auto& proj : gstate.projection) {
        if (proj == 1) {
            gstate.read_src = gstate.has_src;
        } else if (proj == 2) {
            gstate.read_dst = gstate.has_dst;
        }
    }

    if (bind_data.vid_ranges.empty()) {
        gstate.ranges.push_back({0, bind_data.vertex_count});
    } else {
        for (const auto& r : bind_data.vid_ranges) {
            if (r.first >= 0 && r.first < bind_data.vertex_count) {
                gstate.ranges.push_back(r);
            }
        }
        if (gstate.ranges.empty()) {
            gstate.ranges.push_back({0, 0});
        }
    }
    gstate.iter = gstate.ranges.front().first;
    gstate.end_iter = gstate.ranges.front().second;

    auto state = make_uniq<DegreeGlobalTableFunctionState>();
    state->state = std::move(gstate);
    return std::move(state);
}
//-------------------------------------------------------------------
// Execute
//-------------------------------------------------------------------
void Degree::Execute(ClientContext& context, TableFunctionInput& input, DataChunk& output) {
    bool time_logging = GraphArSettings::is_time_logging(context);

    ScopedTimer t("Execute");

    DUCKDB_GRAPHAR_LOG_TRACE("Degree::Execute");

    DegreeGlobalState& gstate = input.global_state->Cast<DegreeGlobalTableFunctionState>().GetState();

    // Projection indices within output (or -1 if a logical column is absent).
    int64_t proj_vid = -1, proj_out = -1, proj_in = -1;
    for (idx_t i = 0; i < gstate.projection.size(); ++i) {
        if (gstate.projection[i] == 0) {
            proj_vid = i;
        } else if (gstate.projection[i] == 1) {
            proj_out = i;
        } else if (gstate.projection[i] == 2) {
            proj_in = i;
        }
    }

    idx_t written = 0;
    while (written < STANDARD_VECTOR_SIZE) {
        if (gstate.cur_range >= gstate.ranges.size()) {
            break;
        }
        const auto& range = gstate.ranges[gstate.cur_range];
        if (gstate.iter >= range.second) {
            gstate.cur_range++;
            if (gstate.cur_range < gstate.ranges.size()) {
                gstate.iter = gstate.ranges[gstate.cur_range].first;
            }
            continue;
        }
        idx_t n = std::min((idx_t)(STANDARD_VECTOR_SIZE - written), (idx_t)(range.second - gstate.iter));

        std::vector<int64_t> out_degs;
        std::vector<int64_t> in_degs;
        if (gstate.read_src) {
            out_degs = ReadDegrees(gstate, graphar::AdjListType::ordered_by_source, gstate.iter, gstate.iter + n);
        }
        if (gstate.read_dst) {
            in_degs = ReadDegrees(gstate, graphar::AdjListType::ordered_by_dest, gstate.iter, gstate.iter + n);
        }

        for (idx_t i = 0; i < n; ++i) {
            if (proj_vid >= 0) {
                output.SetValue(proj_vid, written + i, static_cast<int64_t>(gstate.iter + i));
            }
            if (proj_out >= 0) {
                if (gstate.read_src) {
                    output.SetValue(proj_out, written + i, out_degs[i]);
                } else {
                    output.SetValue(proj_out, written + i, Value());
                }
            }
            if (proj_in >= 0) {
                if (gstate.read_dst) {
                    output.SetValue(proj_in, written + i, in_degs[i]);
                } else {
                    output.SetValue(proj_in, written + i, Value());
                }
            }
        }

        written += n;
        gstate.iter += n;
    }

    output.SetCardinality(written);
    gstate.chunk_count++;

    DUCKDB_GRAPHAR_LOG_DEBUG("Degree::Execute wrote " + std::to_string(written) + " rows");
    if (time_logging) {
        t.print();
    }
}
//-------------------------------------------------------------------
// PushdownComplexFilter
//-------------------------------------------------------------------
void Degree::PushdownComplexFilter(ClientContext& context, LogicalGet& get, FunctionData* bind_data,
                                   vector<unique_ptr<Expression>>& filters) {
    DUCKDB_GRAPHAR_LOG_TRACE("Degree::PushdownComplexFilter");
    if (!bind_data) {
        throw InternalException("Bind data is nullptr");
    }
    auto degree_bind_data = dynamic_cast<DegreeBindData*>(bind_data);
    if (!degree_bind_data) {
        throw InternalException("Degree bind data cast failed");
    }
    if (!degree_bind_data->vid_ranges.empty()) {
        return;
    }

    auto validate = [](const std::string& col, const Value& val) -> bool { return col == GID_COLUMN_INTERNAL; };

    std::set<int64_t> vids;
    vector<unique_ptr<Expression>> filters_new;
    bool already_pushed = false;

    auto validate_wrapper = [&](const std::string& col, const Value& val) -> bool {
        if (validate(col, val)) {
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
        if (filter->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION &&
            BoundComparisonExpression::IsComparison(*filter)) {
            auto& comp_expr = filter->Cast<BoundFunctionExpression>();
            if (comp_expr.GetExpressionType() == ExpressionType::COMPARE_EQUAL) {
                auto& left = BoundComparisonExpression::Left(comp_expr);
                auto& right = BoundComparisonExpression::Right(comp_expr);
                bool left_is_scalar = left.IsFoldable();
                bool right_is_scalar = right.IsFoldable();
                if (left_is_scalar || right_is_scalar) {
                    bool column_on_left = left.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF;
                    auto column_name = column_on_left ? left.ToString() : right.ToString();
                    Value val;
                    auto& scalar_expr = column_on_left ? right : left;
                    if (!ExpressionExecutor::TryEvaluateScalar(context, scalar_expr, val)) {
                        continue;
                    }
                    if (validate_wrapper(column_name, val)) {
                        can_pushdown = true;
                    }
                }
            }
        }

        // Case 1: list_contains([1, 2, 3], col)
        if (!can_pushdown && filter->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
            auto& op_expr = filter->Cast<BoundFunctionExpression>();
            const auto& fname = op_expr.Function().GetName().GetIdentifierName();
            if (fname == "contains" || fname == "list_contains" || fname == "array_contains" || fname == "list_has" ||
                fname == "array_has") {
                auto& children = op_expr.GetChildren();
                if (children.size() == 2 && children[0]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
                    auto& const_expr = children[0]->Cast<BoundConstantExpression>();
                    auto column_name = children[1]->ToString();
                    auto& list_value = const_expr.GetValue();
                    if (list_value.type().id() == LogicalTypeId::LIST) {
                        auto list_children = ListValue::GetChildren(list_value);
                        bool any = false;
                        for (const auto& child : list_children) {
                            if (validate_wrapper(column_name, child)) {
                                any = true;
                            }
                        }
                        if (any) {
                            can_pushdown = true;
                        }
                    }
                }
            }
        }

        // Case 2: col IN (1, 2, 3)
        if (!can_pushdown && filter->GetExpressionClass() == ExpressionClass::BOUND_OPERATOR &&
            filter->GetExpressionType() == ExpressionType::COMPARE_IN) {
            auto& op_expr = filter->Cast<BoundOperatorExpression>();
            auto& children = op_expr.GetChildren();
            if (children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
                auto column_name = children[0]->ToString();
                bool any = false;
                for (idx_t i = 1; i < children.size(); i++) {
                    if (children[i]->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
                        auto& cv = children[i]->Cast<BoundConstantExpression>().GetValue();
                        if (validate_wrapper(column_name, cv)) {
                            any = true;
                        }
                    }
                }
                if (any) {
                    can_pushdown = true;
                }
            }
        }

        // Case 3: col = v1 OR col = v2 OR ... (small IN lists rewritten)
        if (!can_pushdown && filter->GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION &&
            filter->GetExpressionType() == ExpressionType::CONJUNCTION_OR) {
            auto& conj = filter->Cast<BoundConjunctionExpression>();
            std::string column_name;
            std::vector<Value> local_vals;
            bool valid = true;
            for (auto& child : conj.GetChildren()) {
                if (child->GetExpressionClass() != ExpressionClass::BOUND_FUNCTION ||
                    !BoundComparisonExpression::IsComparison(*child) ||
                    child->GetExpressionType() != ExpressionType::COMPARE_EQUAL) {
                    valid = false;
                    break;
                }
                auto& comp_expr = child->Cast<BoundFunctionExpression>();
                std::string col;
                Value val;
                auto& left = BoundComparisonExpression::Left(comp_expr);
                auto& right = BoundComparisonExpression::Right(comp_expr);
                if (left.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
                    right.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
                    col = left.ToString();
                    val = right.Cast<BoundConstantExpression>().GetValue();
                } else if (right.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
                           left.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
                    col = right.ToString();
                    val = left.Cast<BoundConstantExpression>().GetValue();
                } else {
                    valid = false;
                    break;
                }
                if (column_name.empty()) {
                    column_name = col;
                } else if (column_name != col) {
                    valid = false;
                    break;
                }
                local_vals.push_back(val);
            }
            if (valid && !column_name.empty()) {
                bool any = false;
                for (auto& v : local_vals) {
                    if (validate_wrapper(column_name, v)) {
                        any = true;
                    }
                }
                if (any) {
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
        for (const auto& vid : vids) {
            if (0 <= vid && vid < degree_bind_data->vertex_count) {
                degree_bind_data->vid_ranges.push_back({vid, vid + 1});
            }
        }
    }

    filters = std::move(filters_new);
}
//-------------------------------------------------------------------
// GetFunction
//-------------------------------------------------------------------
TableFunction Degree::GetFunction() {
    TableFunction degree(Identifier("degree"), {LogicalType::VARCHAR}, Execute, Bind);
    degree.init_global = DegreeGlobalTableFunctionState::Init;

    degree.filter_pushdown = false;
    degree.projection_pushdown = true;
    degree.pushdown_complex_filter = Degree::PushdownComplexFilter;

    degree.named_parameters["src"] = LogicalType::VARCHAR;
    degree.named_parameters["dst"] = LogicalType::VARCHAR;
    degree.named_parameters["type"] = LogicalType::VARCHAR;

    return degree;
}

void Degree::Register(ExtensionLoader& loader) { loader.RegisterFunction(GetFunction()); }
}  // namespace duckdb