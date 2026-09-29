#pragma once

#include <duckdb/common/types.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/planner/expression.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>

#include <set>
#include <string>
#include <vector>

namespace duckdb {

struct VidFilterPushdownResult {
    bool consumed = false;
    std::string matched_column;
    std::set<int64_t> vids;
};

// Scans `filters` for a _graphAr* index filter (equality, list_contains, IN,
// OR-rewritten IN) and collects the matched vids. If a filter is consumed it is
// removed from `filters` and `consumed` is set; the caller builds the vid
// ranges. `validate(col, val)` tells whether a (column, value) pair is a
// pushable index column.
template <typename ValidateFunc>
VidFilterPushdownResult ExtractVidFilterPushdown(ClientContext& context, vector<unique_ptr<Expression>>& filters,
                                                 ValidateFunc validate) {
    VidFilterPushdownResult result;
    vector<unique_ptr<Expression>> filters_new;
    bool already_pushed = false;

    auto validate_wrapper = [&](const std::string& col, const Value& val) -> bool {
        if (validate(col, val)) {
            if (!val.IsNull()) {
                result.vids.insert(val.GetValue<int64_t>());
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
                        result.matched_column = column_name;
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
                            result.matched_column = column_name;
                            can_pushdown = true;
                        }
                    }
                }
            }
        }

        // Case 2: col IN (1, 2, 3). Only consume the filter when every RHS member is a constant.
        if (!can_pushdown && filter->GetExpressionClass() == ExpressionClass::BOUND_OPERATOR &&
            filter->GetExpressionType() == ExpressionType::COMPARE_IN) {
            auto& op_expr = filter->Cast<BoundOperatorExpression>();
            auto& children = op_expr.GetChildren();
            if (children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
                auto column_name = children[0]->ToString();
                bool all_const = true;
                for (idx_t i = 1; i < children.size(); i++) {
                    if (children[i]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
                        all_const = false;
                        break;
                    }
                }
                if (all_const) {
                    bool any = false;
                    for (idx_t i = 1; i < children.size(); i++) {
                        auto& cv = children[i]->Cast<BoundConstantExpression>().GetValue();
                        if (validate_wrapper(column_name, cv)) {
                            any = true;
                        }
                    }
                    if (any) {
                        result.matched_column = column_name;
                        can_pushdown = true;
                    }
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
                    result.matched_column = column_name;
                    can_pushdown = true;
                }
            }
        }

        if (!can_pushdown) {
            filters_new.push_back(std::move(filter));
        } else {
            result.consumed = true;
            already_pushed = true;
        }
    }

    filters = std::move(filters_new);
    return result;
}

}  // namespace duckdb