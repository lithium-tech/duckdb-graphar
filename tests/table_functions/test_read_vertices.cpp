#define CATCH_CONFIG_MAIN
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/benchmark/catch_benchmark.hpp>

#include <filesystem>
#include <iostream>

#include "table_functions_fixture.hpp"
#include "functions/table/read_vertices.hpp"

using namespace duckdb;
using namespace graphar;

#define TestFixture TableFunctionsFixture<TestType>

TEST_CASE("ReadVertices GetFunction basic test", "[read_vertices]") {
    TableFunction read_vertices;
    REQUIRE_NOTHROW(read_vertices = ReadVertices::GetFunction());
    
    REQUIRE(read_vertices.name == "read_vertices");
    REQUIRE(read_vertices.arguments.size() == 1);
    REQUIRE(read_vertices.named_parameters.size() == 1);
    CHECK(read_vertices.filter_pushdown == false);
    CHECK(read_vertices.projection_pushdown == true);

    REQUIRE(read_vertices.named_parameters.find("type") != read_vertices.named_parameters.end());
}

TEMPLATE_TEST_CASE_METHOD(TableFunctionsFixture, "ReadVertices Bind and Execute functions vertex without properties", "[read_vertices]", FILE_TYPES_FOR_TEST) {
    INFO("Start mocking");
    vector<Value> inputs({Value(TestFixture::path_trial_graph)});
    named_parameter_map_t named_parameters({{"type", Value("Person")}});
    vector<LogicalType> input_table_types({});
    auto input = TestFixture::CreateMockBindInput(inputs, named_parameters, input_table_types);

    vector<LogicalType> return_types;
    vector<std::string> names;
    INFO("Finish mocking");
    
    TableFunction read_vertices = ReadVertices::GetFunction();

    INFO("Bind test");
    unique_ptr<FunctionData> bind_data;
    REQUIRE_NOTHROW(bind_data = read_vertices.bind(*TestFixture::conn.context, input, return_types, names));

    REQUIRE(bind_data != nullptr);
    REQUIRE(return_types == vector<LogicalType>({LogicalType::BIGINT, LogicalType::INTEGER}));
    REQUIRE(names == vector<std::string>({GID_COLUMN_INTERNAL, "hash_phone_no"}));
    INFO("Finish bind test");

    TableFunctionInitInput func_init_input(bind_data.get(), vector<column_t>(), {}, nullptr);

    unique_ptr<GlobalTableFunctionState> gstate;
    REQUIRE_NOTHROW(gstate = read_vertices.init_global(*TestFixture::conn.context, func_init_input));

    TableFunctionInput func_input(bind_data.get(), nullptr, gstate.get());

    DataChunk res;
    res.Initialize(*TestFixture::conn.context, return_types);
    DataChunk tmp;
    tmp.Initialize(*TestFixture::conn.context, return_types);

    INFO("Execute test");
    REQUIRE_NOTHROW(read_vertices.function(*TestFixture::conn.context, func_input, tmp));
    while (tmp.size() > 0){
        INFO("Size of the result part: " << tmp.size());
        REQUIRE_NOTHROW(res.Append(tmp, true));
        tmp.Reset();
        REQUIRE_NOTHROW(read_vertices.function(*TestFixture::conn.context, func_input, tmp));
    }

    INFO("Checking results");
    REQUIRE(res.size() == 5);
    REQUIRE(res.ColumnCount() == 2);
    INFO("Finish execute test");
}

TEMPLATE_TEST_CASE_METHOD(TableFunctionsFixture, "ReadVertices Bind function invalid_vertex", "[read_vertices]", FILE_TYPES_FOR_TEST) {
    INFO("Start mocking");
    vector<Value> inputs({Value(TestFixture::path_trial_graph)});
    named_parameter_map_t named_parameters({{"type", Value("InvalidVertex")}});
    vector<LogicalType> input_table_types({});
    auto input = TestFixture::CreateMockBindInput(inputs, named_parameters, input_table_types);

    vector<LogicalType> return_types;
    vector<std::string> names;
    INFO("Finish mocking");

    TableFunction read_vertices = ReadVertices::GetFunction();

    REQUIRE_THROWS_AS(read_vertices.bind(*TestFixture::conn.context, input, return_types, names), BinderException);
}

/* // Uncomment after fixing SIGSEGV
TEMPLATE_TEST_CASE_METHOD(TableFunctionsFixture,"ReadVertices Bind and Execute functions vertex with properties", "[read_vertices]", FILE_TYPES_FOR_TEST) {
    INFO("Start mocking");
    vector<Value> inputs({Value(TestFixture::path_feature_graph)});
    
    INFO("Path: " + TestFixture::path_feature_graph);

    named_parameter_map_t named_parameters({{"type", Value("Person")}});
    vector<LogicalType> input_table_types({});
    auto input = TestFixture::CreateMockBindInput(inputs, named_parameters, input_table_types);

    vector<LogicalType> return_types;
    vector<std::string> names;
    INFO("Finish mocking");
    
    TableFunction read_vertices = ReadVertices::GetFunction();

    INFO("Bind test");
    unique_ptr<FunctionData> bind_data;
    REQUIRE_NOTHROW(bind_data = read_vertices.bind(*TestFixture::conn.context, input, return_types, names));

    REQUIRE(bind_data != nullptr);
    REQUIRE(return_types == vector<LogicalType>({LogicalType::BIGINT, LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::VARCHAR}));
    REQUIRE(names == vector<std::string>({GID_COLUMN_INTERNAL, "hash_phone_no", "first_name", "last_name"}));
    INFO("Finish bind test");

    TableFunctionInitInput func_init_input(bind_data.get(), vector<column_t>(), {}, nullptr);

    unique_ptr<GlobalTableFunctionState> gstate;
    REQUIRE_NOTHROW(gstate = read_vertices.init_global(*TestFixture::conn.context, func_init_input));

    TableFunctionInput func_input(bind_data.get(), nullptr, gstate.get());

    DataChunk res;
    res.Initialize(*TestFixture::conn.context, return_types);    
    DataChunk tmp;
    tmp.Initialize(*TestFixture::conn.context, return_types);

    INFO("Execute test");
    REQUIRE_NOTHROW(read_vertices.function(*TestFixture::conn.context, func_input, res));
    while (tmp.size() > 0){
        INFO("Size of the result part: " << tmp.size());
        REQUIRE_NOTHROW(res.Append(tmp, true));

        REQUIRE_NOTHROW(read_vertices.function(*TestFixture::conn.context, func_input, tmp));
    }

    INFO("Checking results");
    REQUIRE(res.size() == 5);
    REQUIRE(res.ColumnCount() == 4);
    INFO("Finish execute test");
}
*/