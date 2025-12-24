#define CATCH_CONFIG_MAIN
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/benchmark/catch_benchmark.hpp>

#include <filesystem>  
#include <iostream>  

#include "table_functions_fixture.hpp"
#include "functions/table/read_edges.hpp"

using namespace duckdb;
using namespace graphar;

#define TestFixture TableFunctionsFixture<TestType>

TEST_CASE("ReadEdges GetFunction basic test", "[read_edges]") {
    TableFunction read_edges;
    REQUIRE_NOTHROW(read_edges = ReadEdges::GetFunction());
    
    REQUIRE(read_edges.name == "read_edges");
    REQUIRE(read_edges.arguments.size() == 1);
    REQUIRE(read_edges.named_parameters.size() == 3);
    CHECK(read_edges.filter_pushdown == false);
    CHECK(read_edges.projection_pushdown == true);
    
    REQUIRE(read_edges.named_parameters.find("src") != read_edges.named_parameters.end());
    REQUIRE(read_edges.named_parameters.find("dst") != read_edges.named_parameters.end());
    REQUIRE(read_edges.named_parameters.find("type") != read_edges.named_parameters.end());
}

TEMPLATE_TEST_CASE_METHOD(TableFunctionsFixture, "ReadEdges Bind function invalid_edge", "[read_edges]", FILE_TYPES_FOR_TEST) {
    INFO("Start mocking");
    vector<Value> inputs({Value(TestFixture::path_trial_graph)});

    INFO("Path: " + TestFixture::path_trial_graph);

    named_parameter_map_t named_parameters({{"src", Value("Person")}, {"dst", Value("Person")}, {"type", Value("invalid_edge")}});
    vector<LogicalType> input_table_types({});
    auto input = TestFixture::CreateMockBindInput(inputs, named_parameters, input_table_types);    
        
    vector<LogicalType> return_types;
    vector<std::string> names;
    INFO("Finish mocking");
    
    TableFunction read_edges = ReadEdges::GetFunction();

    REQUIRE_THROWS_AS(read_edges.bind(*TestFixture::conn.context, input, return_types, names), BinderException);
}

TEMPLATE_TEST_CASE_METHOD(TableFunctionsFixture, "ReadEdges Bind and Execute functions edge without property", "[read_edges]", FILE_TYPES_FOR_TEST) {

    INFO("Start mocking");    
    vector<Value> inputs({Value(TestFixture::path_trial_graph)});

    INFO("Path: " + TestFixture::path_trial_graph);

    named_parameter_map_t named_parameters({{"src", Value("Person")}, {"dst", Value("Person")}, {"type", Value("knows")}});
    vector<LogicalType> input_table_types({});
    auto input = TestFixture::CreateMockBindInput(inputs, named_parameters, input_table_types);    
    
    vector<LogicalType> return_types;
    vector<std::string> names;
    INFO("Finish mocking");

    TableFunction read_edges = ReadEdges::GetFunction();
 
    INFO("Bind test");    
    unique_ptr<FunctionData> bind_data;
    REQUIRE_NOTHROW(bind_data = read_edges.bind(*TestFixture::conn.context, input, return_types, names));

    REQUIRE(bind_data != nullptr);
    REQUIRE(return_types == vector<LogicalType> ({LogicalType::BIGINT, LogicalType::BIGINT}));
    REQUIRE(names == vector<std::string> ({SRC_GID_COLUMN, DST_GID_COLUMN}));
    INFO("Finish bind test");

    TableFunctionInitInput func_init_input(bind_data.get(), vector<column_t>(), {}, nullptr);

    unique_ptr<GlobalTableFunctionState> gstate;
    REQUIRE_NOTHROW(gstate = read_edges.init_global(*TestFixture::conn.context, func_init_input));
    
    TableFunctionInput func_input(bind_data.get(), nullptr, gstate);

    DataChunk res;
    res.Initialize(*TestFixture::conn.context, return_types);
    DataChunk tmp;
    tmp.Initialize(*TestFixture::conn.context, return_types);

    INFO("Execute test");
    REQUIRE_NOTHROW(read_edges.function(*TestFixture::conn.context, func_input, tmp));
    while (tmp.size() > 0){
        INFO("Size of the result part: " << tmp.size());
        REQUIRE_NOTHROW(res.Append(tmp, true));
        tmp.Reset();
        REQUIRE_NOTHROW(read_edges.function(*TestFixture::conn.context, func_input, tmp));
    }

    INFO("Checking results");
    REQUIRE(res.size() == 7);
    REQUIRE(res.ColumnCount() == 2);
    INFO("Finish execute test");
}
/* // Uncomment after fixing SIGSEGV
TEMPLATE_TEST_CASE_METHOD(TableFunctionsFixture, "ReadEdges Bind and Execute functions edge with property", "[read_edges]", FILE_TYPES_FOR_TEST) {
    INFO("Start mocking");
    vector<Value> inputs({Value(TestFixture::path_feature_graph)});

    INFO("Path: " + TestFixture::path_feature_graph);

    named_parameter_map_t named_parameters({{"src", Value("Person")}, {"dst", Value("Person")}, {"type", Value("knows")}});
    vector<LogicalType> input_table_types({});
    auto input = TestFixture::CreateMockBindInput(inputs, named_parameters, input_table_types);    
    
    vector<LogicalType> return_types;
    vector<std::string> names;
    INFO("Finish mocking");

    TableFunction read_edges = ReadEdges::GetFunction();

    INFO("Bind test"); 
    unique_ptr<FunctionData> bind_data;
    REQUIRE_NOTHROW(bind_data = read_edges.bind(*TestFixture::conn.context, input, return_types, names));

    REQUIRE(bind_data != nullptr);
    REQUIRE(return_types == vector<LogicalType> ({LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::FLOAT}));
    REQUIRE(names == vector<std::string> ({SRC_GID_COLUMN, DST_GID_COLUMN, "friend_score", "created_at", "tmp_"}));
    INFO("Finish bind test");

    TableFunctionInitInput func_init_input(bind_data.get(), vector<column_t>(), {}, nullptr);
    
    unique_ptr<GlobalTableFunctionState> gstate;
    REQUIRE_NOTHROW(gstate = read_edges.init_global(*TestFixture::conn.context, func_init_input));
    
    TableFunctionInput func_input(bind_data.get(), nullptr, gstate.get());

    DataChunk res;
    res.Initialize(*TestFixture::conn.context, return_types);
    DataChunk tmp;
    tmp.Initialize(*TestFixture::conn.context, return_types);
    
    INFO("Execute test");
    REQUIRE_NOTHROW(read_edges.function(*TestFixture::conn.context, func_input, tmp));
    while (tmp.size() > 0){
        INFO("Size of the result part: " << tmp.size());
        REQUIRE_NOTHROW(res.Append(tmp, true));

        tmp.Reset();
        REQUIRE_NOTHROW(read_edges.function(*TestFixture::conn.context, func_input, tmp));
    }

    INFO("Checking results");
    REQUIRE(res.size() == 7);
    REQUIRE(res.ColumnCount() == 5);
    INFO("Finish execute test");
}
*/