#include "utils/func.hpp"

#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <duckdb/common/exception.hpp>

#include <graphar/expression.h>

#include <catch2/catch_test_macros.hpp>

using namespace duckdb;

// GraphArFunctions::GetFilter builds a graphar expression that DuckDB pushes
// down to GraphAr as an Arrow filter. It must support every property type that
// the extension can map (see graphArT2duckT / graphArT2arrowT), including the
// int16 and bool types added when int16 support landed.
TEST_CASE ("GraphArFunctions::GetFilter supports int16", "[graphar_func]") {
    auto filter = GraphArFunctions::GetFilter("int16", "42", "age");
    REQUIRE(filter != nullptr);
    REQUIRE_FALSE(filter->Evaluate().has_error());
}

TEST_CASE ("GraphArFunctions::GetFilter supports bool", "[graphar_func]") {
    auto filter_true = GraphArFunctions::GetFilter("bool", "true", "active");
    REQUIRE(filter_true != nullptr);
    REQUIRE_FALSE(filter_true->Evaluate().has_error());

    auto filter_false = GraphArFunctions::GetFilter("bool", "false", "active");
    REQUIRE(filter_false != nullptr);
    REQUIRE_FALSE(filter_false->Evaluate().has_error());
}

TEST_CASE ("GraphArFunctions::GetFilter rejects invalid bool value", "[graphar_func]") {
    REQUIRE_THROWS_AS(GraphArFunctions::GetFilter("bool", "yes", "active"), InvalidInputException);
}

TEST_CASE ("GraphArFunctions::GetFilter rejects unsupported type", "[graphar_func]") {
    REQUIRE_THROWS_AS(GraphArFunctions::GetFilter("not_a_type", "1", "col"), NotImplementedException);
}

// int16 is promoted to int32 (graphar::_Literal has no int16 overload). Verify
// that the produced Arrow filter compares the "age" property against an int32
// literal (not, e.g., int64 or a mangled value).
// List types are parsed recursively: "list<T>" maps to DuckDB LogicalType::LIST
// and to Arrow large_list. These must cover every list element type GraphAr
// supports (see graphArT2duckT / graphArT2arrowT).
TEST_CASE ("GraphArFunctions::graphArT2duckT supports list types", "[graphar_func]") {
    REQUIRE(GraphArFunctions::graphArT2duckT("list<int16>") == LogicalType::LIST(LogicalType(LogicalTypeId::SMALLINT)));
    REQUIRE(GraphArFunctions::graphArT2duckT("list<int32>") == LogicalType::LIST(LogicalType(LogicalTypeId::INTEGER)));
    REQUIRE(GraphArFunctions::graphArT2duckT("list<int64>") == LogicalType::LIST(LogicalType(LogicalTypeId::BIGINT)));
    REQUIRE(GraphArFunctions::graphArT2duckT("list<float>") == LogicalType::LIST(LogicalType(LogicalTypeId::FLOAT)));
    REQUIRE(GraphArFunctions::graphArT2duckT("list<double>") == LogicalType::LIST(LogicalType(LogicalTypeId::DOUBLE)));
    REQUIRE(GraphArFunctions::graphArT2duckT("list<string>") == LogicalType::LIST(LogicalType(LogicalTypeId::VARCHAR)));
}

TEST_CASE ("GraphArFunctions::graphArT2arrowT supports list types", "[graphar_func]") {
    REQUIRE(GraphArFunctions::graphArT2arrowT("list<int16>")->Equals(*arrow::large_list(arrow::int16())));
    REQUIRE(GraphArFunctions::graphArT2arrowT("list<int32>")->Equals(*arrow::large_list(arrow::int32())));
    REQUIRE(GraphArFunctions::graphArT2arrowT("list<int64>")->Equals(*arrow::large_list(arrow::int64())));
    REQUIRE(GraphArFunctions::graphArT2arrowT("list<float>")->Equals(*arrow::large_list(arrow::float32())));
    REQUIRE(GraphArFunctions::graphArT2arrowT("list<double>")->Equals(*arrow::large_list(arrow::float64())));
    REQUIRE(GraphArFunctions::graphArT2arrowT("list<string>")->Equals(*arrow::large_list(arrow::utf8())));
}

TEST_CASE ("GraphArFunctions::graphArT2duckT rejects malformed list type", "[graphar_func]") {
    REQUIRE_THROWS_AS(GraphArFunctions::graphArT2duckT("list<"), NotImplementedException);
    REQUIRE_THROWS_AS(GraphArFunctions::graphArT2duckT("list<list<"), NotImplementedException);
}

TEST_CASE ("GraphArFunctions::GetFilter int16 promotes to int32 literal", "[graphar_func]") {
    auto filter = GraphArFunctions::GetFilter("int16", "42", "age");
    auto expr_result = filter->Evaluate();
    REQUIRE_FALSE(expr_result.has_error());
    auto expr = expr_result.value();

    // GetFilter builds equal(age, <literal>). Inspect the literal argument.
    auto call = expr.call();
    REQUIRE(call != nullptr);
    REQUIRE(call->function_name == "equal");
    REQUIRE(call->arguments.size() == 2);

    // argument[0] is the field reference to the property.
    auto field_ref = call->arguments[0].field_ref();
    REQUIRE(field_ref != nullptr);
    auto name = field_ref->name();
    REQUIRE(name != nullptr);
    REQUIRE(*name == "age");

    // argument[1] is the promoted literal; it must be an int32.
    auto literal = call->arguments[1].literal();
    REQUIRE(literal != nullptr);
    REQUIRE(literal->type()->Equals(*arrow::int32()));
}
