#define CATCH_CONFIG_MAIN
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/benchmark/catch_benchmark.hpp>

#include <filesystem>
#include <iostream>

#include "table_functions_fixture.hpp"
#include "functions/table/shortest_path.hpp"

using namespace duckdb;
using namespace graphar;

// Trial Graph Structure (used in most tests):
// Vertices: 0, 1, 2, 3, 4, 5
// Edges (directed): 1->2, 1->3, 2->3, 2->4, 3->4, 3->5, 4->5
//
// Visual representation:
//   0 (isolated)
//   1 -> 2 -> 3 -> 5
//        |
//        v
//        4
//
// Multi-component Graph:
// Component 1: 0->1->2->3->4 (chain)
// Component 2: 5->7->9 (chain)
// Component 3: 8->6 (chain)

#define TestFixture TableFunctionsFixture<TestType>

TEST_CASE("ShortestPath GetFunction basic test", "[shortest_path]") {
    TableFunction shortest_path_func;
    REQUIRE_NOTHROW(shortest_path_func = ShortestPath::GetFunction());
    
    REQUIRE(shortest_path_func.name == "shortest_path");
    REQUIRE(shortest_path_func.arguments.size() == 3);
    REQUIRE(shortest_path_func.named_parameters.size() == 3);
    CHECK(shortest_path_func.filter_pushdown == false);
    CHECK(shortest_path_func.projection_pushdown == false);

    REQUIRE(shortest_path_func.named_parameters.find("src") != shortest_path_func.named_parameters.end());
    REQUIRE(shortest_path_func.named_parameters.find("type") != shortest_path_func.named_parameters.end());
    REQUIRE(shortest_path_func.named_parameters.find("dst") != shortest_path_func.named_parameters.end());
}

TEMPLATE_TEST_CASE_METHOD(TableFunctionsFixture, "ShortestPath Bind and Execute - 1 hop path", "[shortest_path]", FILE_TYPES_FOR_TEST) {
    INFO("Start mocking data for bind");
    // Path: 1 -> 2 exists in trial graph
    // New signature: begin_id, end_id, yaml_path with named params src/type/dst
    vector<Value> inputs({Value::BIGINT(1), Value::BIGINT(2), Value(TestFixture::path_trial_graph)});

    INFO("Path graph: " + TestFixture::path_trial_graph);

    named_parameter_map_t named_parameters({
        {"src", Value("Person")},
        {"type", Value("knows")},
        {"dst", Value("Person")}
    });
    vector<LogicalType> input_table_types({});
    auto bind_input = TestFixture::CreateMockBindInput(inputs, named_parameters, input_table_types);

    vector<LogicalType> return_types;
    vector<std::string> names;
    INFO("Finish mocking");

    TableFunction shortest_path_func = ShortestPath::GetFunction();

    INFO("Bind test");
    TestFixture::conn.BeginTransaction();
    unique_ptr<FunctionData> bind_data;
    REQUIRE_NOTHROW(bind_data = shortest_path_func.bind(*TestFixture::conn.context, bind_input, return_types, names));

    REQUIRE(bind_data != nullptr);
    REQUIRE(names == vector<std::string>({"step_number", "_graphArVertexIndex"}));
    REQUIRE(return_types == vector<LogicalType>({LogicalType::BIGINT, LogicalType::BIGINT}));
    INFO("Finish bind test");

    TableFunctionInitInput func_init_input(bind_data.get(), vector<column_t>(), {}, nullptr);
    INFO("Prepare func_init_input");

    unique_ptr<GlobalTableFunctionState> gstate;
    REQUIRE_NOTHROW(gstate = shortest_path_func.init_global(*TestFixture::conn.context, func_init_input));
    INFO("Finish init global state");

    TableFunctionInput func_input(bind_data.get(), nullptr, gstate);
    INFO("Prepare func_input");

    DataChunk res;
    res.Initialize(*TestFixture::conn.context, return_types);
    DataChunk tmp;
    tmp.Initialize(*TestFixture::conn.context, return_types);

    INFO("Execute test");
    REQUIRE_NOTHROW(shortest_path_func.function(*TestFixture::conn.context, func_input, tmp));
    while (tmp.size() > 0){
        res.Append(tmp, VectorAppendMode::ALLOW_RESIZE);
        tmp.Reset();
        REQUIRE_NOTHROW(shortest_path_func.function(*TestFixture::conn.context, func_input, tmp));
    }
    if(tmp.size() > 0) res.Append(tmp, VectorAppendMode::ALLOW_RESIZE);

    INFO("Checking results");
    REQUIRE(res.size() == 2); // step 0: vertex 1, step 1: vertex 2
    REQUIRE(res.ColumnCount() == 2);
    
    auto step_data = FlatVector::GetData<int64_t>(res.data[0]);
    auto vertex_data = FlatVector::GetData<int64_t>(res.data[1]);
    
    REQUIRE(step_data[0] == 0);
    REQUIRE(vertex_data[0] == 1);
    REQUIRE(step_data[1] == 1);
    REQUIRE(vertex_data[1] == 2);
    INFO("Finish execute test");
}

TEMPLATE_TEST_CASE_METHOD(TableFunctionsFixture, "ShortestPath Bind and Execute - 2 hop path", "[shortest_path]", FILE_TYPES_FOR_TEST) {
    INFO("Start mocking data for bind");
    // Path: 1 -> 2 -> 4 exists in trial graph
    vector<Value> inputs({Value::BIGINT(1), Value::BIGINT(4), Value(TestFixture::path_trial_graph)});

    INFO("Path graph: " + TestFixture::path_trial_graph);

    named_parameter_map_t named_parameters({
        {"src", Value("Person")},
        {"type", Value("knows")},
        {"dst", Value("Person")}
    });
    vector<LogicalType> input_table_types({});
    auto bind_input = TestFixture::CreateMockBindInput(inputs, named_parameters, input_table_types);

    vector<LogicalType> return_types;
    vector<std::string> names;
    INFO("Finish mocking");

    TableFunction shortest_path_func = ShortestPath::GetFunction();

    INFO("Bind test");
    TestFixture::conn.BeginTransaction();
    unique_ptr<FunctionData> bind_data;
    REQUIRE_NOTHROW(bind_data = shortest_path_func.bind(*TestFixture::conn.context, bind_input, return_types, names));

    REQUIRE(bind_data != nullptr);
    REQUIRE(names == vector<std::string>({"step_number", "_graphArVertexIndex"}));
    REQUIRE(return_types == vector<LogicalType>({LogicalType::BIGINT, LogicalType::BIGINT}));
    INFO("Finish bind test");

    TableFunctionInitInput func_init_input(bind_data.get(), vector<column_t>(), {}, nullptr);
    INFO("Prepare func_init_input");

    unique_ptr<GlobalTableFunctionState> gstate;
    REQUIRE_NOTHROW(gstate = shortest_path_func.init_global(*TestFixture::conn.context, func_init_input));
    INFO("Finish init global state");

    TableFunctionInput func_input(bind_data.get(), nullptr, gstate);
    INFO("Prepare func_input");

    DataChunk res;
    res.Initialize(*TestFixture::conn.context, return_types);
    DataChunk tmp;
    tmp.Initialize(*TestFixture::conn.context, return_types);

    INFO("Execute test");
    REQUIRE_NOTHROW(shortest_path_func.function(*TestFixture::conn.context, func_input, tmp));
    while (tmp.size() > 0){
        res.Append(tmp, VectorAppendMode::ALLOW_RESIZE);
        tmp.Reset();
        REQUIRE_NOTHROW(shortest_path_func.function(*TestFixture::conn.context, func_input, tmp));
    }
    if(tmp.size() > 0) res.Append(tmp, VectorAppendMode::ALLOW_RESIZE);

    INFO("Checking results");
    REQUIRE(res.size() == 3); // step 0: vertex 1, step 1: vertex 2, step 2: vertex 4
    REQUIRE(res.ColumnCount() == 2);
    
    auto step_data = FlatVector::GetData<int64_t>(res.data[0]);
    auto vertex_data = FlatVector::GetData<int64_t>(res.data[1]);
    
    REQUIRE(step_data[0] == 0);
    REQUIRE(vertex_data[0] == 1);
    REQUIRE(step_data[1] == 1);
    REQUIRE(vertex_data[1] == 2);
    REQUIRE(step_data[2] == 2);
    REQUIRE(vertex_data[2] == 4);
    INFO("Finish execute test");
}

TEMPLATE_TEST_CASE_METHOD(TableFunctionsFixture, "ShortestPath Bind and Execute - same vertex", "[shortest_path]", FILE_TYPES_FOR_TEST) {
    INFO("Start mocking data for bind");
    // Path: 1 -> 1 (same vertex)
    vector<Value> inputs({Value::BIGINT(1), Value::BIGINT(1), Value(TestFixture::path_trial_graph)});

    INFO("Path graph: " + TestFixture::path_trial_graph);

    named_parameter_map_t named_parameters({
        {"src", Value("Person")},
        {"type", Value("knows")},
        {"dst", Value("Person")}
    });
    vector<LogicalType> input_table_types({});
    auto bind_input = TestFixture::CreateMockBindInput(inputs, named_parameters, input_table_types);

    vector<LogicalType> return_types;
    vector<std::string> names;
    INFO("Finish mocking");

    TableFunction shortest_path_func = ShortestPath::GetFunction();

    INFO("Bind test");
    TestFixture::conn.BeginTransaction();
    unique_ptr<FunctionData> bind_data;
    REQUIRE_NOTHROW(bind_data = shortest_path_func.bind(*TestFixture::conn.context, bind_input, return_types, names));

    REQUIRE(bind_data != nullptr);
    REQUIRE(names == vector<std::string>({"step_number", "_graphArVertexIndex"}));
    REQUIRE(return_types == vector<LogicalType>({LogicalType::BIGINT, LogicalType::BIGINT}));
    INFO("Finish bind test");

    TableFunctionInitInput func_init_input(bind_data.get(), vector<column_t>(), {}, nullptr);
    INFO("Prepare func_init_input");

    unique_ptr<GlobalTableFunctionState> gstate;
    REQUIRE_NOTHROW(gstate = shortest_path_func.init_global(*TestFixture::conn.context, func_init_input));
    INFO("Finish init global state");

    TableFunctionInput func_input(bind_data.get(), nullptr, gstate);
    INFO("Prepare func_input");

    DataChunk res;
    res.Initialize(*TestFixture::conn.context, return_types);
    DataChunk tmp;
    tmp.Initialize(*TestFixture::conn.context, return_types);

    INFO("Execute test");
    REQUIRE_NOTHROW(shortest_path_func.function(*TestFixture::conn.context, func_input, tmp));
    while (tmp.size() > 0){
        res.Append(tmp, VectorAppendMode::ALLOW_RESIZE);
        tmp.Reset();
        REQUIRE_NOTHROW(shortest_path_func.function(*TestFixture::conn.context, func_input, tmp));
    }
    if(tmp.size() > 0) res.Append(tmp, VectorAppendMode::ALLOW_RESIZE);

    INFO("Checking results");
    REQUIRE(res.size() == 1); // step 0: vertex 1
    REQUIRE(res.ColumnCount() == 2);
    
    auto step_data = FlatVector::GetData<int64_t>(res.data[0]);
    auto vertex_data = FlatVector::GetData<int64_t>(res.data[1]);
    
    REQUIRE(step_data[0] == 0);
    REQUIRE(vertex_data[0] == 1);
    INFO("Finish execute test");
}

TEMPLATE_TEST_CASE_METHOD(TableFunctionsFixture, "ShortestPath Bind and Execute - no path (reverse direction)", "[shortest_path]", FILE_TYPES_FOR_TEST) {
    INFO("Start mocking data for bind");
    // Path: 2 -> 1 does not exist (directed graph)
    vector<Value> inputs({Value::BIGINT(2), Value::BIGINT(1), Value(TestFixture::path_trial_graph)});

    INFO("Path graph: " + TestFixture::path_trial_graph);

    named_parameter_map_t named_parameters({
        {"src", Value("Person")},
        {"type", Value("knows")},
        {"dst", Value("Person")}
    });
    vector<LogicalType> input_table_types({});
    auto bind_input = TestFixture::CreateMockBindInput(inputs, named_parameters, input_table_types);

    vector<LogicalType> return_types;
    vector<std::string> names;
    INFO("Finish mocking");

    TableFunction shortest_path_func = ShortestPath::GetFunction();

    INFO("Bind test");
    TestFixture::conn.BeginTransaction();
    unique_ptr<FunctionData> bind_data;
    REQUIRE_NOTHROW(bind_data = shortest_path_func.bind(*TestFixture::conn.context, bind_input, return_types, names));

    REQUIRE(bind_data != nullptr);
    REQUIRE(names == vector<std::string>({"step_number", "_graphArVertexIndex"}));
    REQUIRE(return_types == vector<LogicalType>({LogicalType::BIGINT, LogicalType::BIGINT}));
    INFO("Finish bind test");

    TableFunctionInitInput func_init_input(bind_data.get(), vector<column_t>(), {}, nullptr);
    INFO("Prepare func_init_input");

    unique_ptr<GlobalTableFunctionState> gstate;
    REQUIRE_NOTHROW(gstate = shortest_path_func.init_global(*TestFixture::conn.context, func_init_input));
    INFO("Finish init global state");

    TableFunctionInput func_input(bind_data.get(), nullptr, gstate);
    INFO("Prepare func_input");

    DataChunk res;
    res.Initialize(*TestFixture::conn.context, return_types);
    DataChunk tmp;
    tmp.Initialize(*TestFixture::conn.context, return_types);

    INFO("Execute test");
    REQUIRE_NOTHROW(shortest_path_func.function(*TestFixture::conn.context, func_input, tmp));
    while (tmp.size() > 0){
        res.Append(tmp, VectorAppendMode::ALLOW_RESIZE);
        tmp.Reset();
        REQUIRE_NOTHROW(shortest_path_func.function(*TestFixture::conn.context, func_input, tmp));
    }
    if(tmp.size() > 0) res.Append(tmp, VectorAppendMode::ALLOW_RESIZE);

    INFO("Checking results");
    REQUIRE(res.size() == 0); // No path exists
    INFO("Finish execute test");
}

TEMPLATE_TEST_CASE_METHOD(TableFunctionsFixture, "ShortestPath Bind and Execute - no path (disconnected component)", "[shortest_path]", FILE_TYPES_FOR_TEST) {
    INFO("Start mocking data for bind");
    // Path: 1 -> 7 does not exist (different components)
    // Component 1: 0-1-2-3-4, Component 2: 5-7-9, Component 3: 8-6
    vector<Value> inputs({Value::BIGINT(1), Value::BIGINT(7), Value(TestFixture::path_multi_component_graph)});

    INFO("Path graph: " + TestFixture::path_multi_component_graph);

    named_parameter_map_t named_parameters({
        {"src", Value("Person")},
        {"type", Value("knows")},
        {"dst", Value("Person")}
    });
    vector<LogicalType> input_table_types({});
    auto bind_input = TestFixture::CreateMockBindInput(inputs, named_parameters, input_table_types);

    vector<LogicalType> return_types;
    vector<std::string> names;
    INFO("Finish mocking");

    TableFunction shortest_path_func = ShortestPath::GetFunction();

    INFO("Bind test");
    TestFixture::conn.BeginTransaction();
    unique_ptr<FunctionData> bind_data;
    REQUIRE_NOTHROW(bind_data = shortest_path_func.bind(*TestFixture::conn.context, bind_input, return_types, names));

    REQUIRE(bind_data != nullptr);
    REQUIRE(names == vector<std::string>({"step_number", "_graphArVertexIndex"}));
    REQUIRE(return_types == vector<LogicalType>({LogicalType::BIGINT, LogicalType::BIGINT}));
    INFO("Finish bind test");

    TableFunctionInitInput func_init_input(bind_data.get(), vector<column_t>(), {}, nullptr);
    INFO("Prepare func_init_input");

    unique_ptr<GlobalTableFunctionState> gstate;
    REQUIRE_NOTHROW(gstate = shortest_path_func.init_global(*TestFixture::conn.context, func_init_input));
    INFO("Finish init global state");

    TableFunctionInput func_input(bind_data.get(), nullptr, gstate);
    INFO("Prepare func_input");

    DataChunk res;
    res.Initialize(*TestFixture::conn.context, return_types);
    DataChunk tmp;
    tmp.Initialize(*TestFixture::conn.context, return_types);

    INFO("Execute test");
    REQUIRE_NOTHROW(shortest_path_func.function(*TestFixture::conn.context, func_input, tmp));
    while (tmp.size() > 0){
        res.Append(tmp, VectorAppendMode::ALLOW_RESIZE);
        tmp.Reset();
        REQUIRE_NOTHROW(shortest_path_func.function(*TestFixture::conn.context, func_input, tmp));
    }
    if(tmp.size() > 0) res.Append(tmp, VectorAppendMode::ALLOW_RESIZE);

    INFO("Checking results");
    REQUIRE(res.size() == 0); // No path exists between disconnected components
    INFO("Finish execute test");
}

TEMPLATE_TEST_CASE_METHOD(TableFunctionsFixture, "ShortestPath Bind and Execute - path in disconnected component", "[shortest_path]", FILE_TYPES_FOR_TEST) {
    INFO("Start mocking data for bind");
    // Path: 0 -> 1 -> 2 exists in multi-component graph (component 1)
    vector<Value> inputs({Value::BIGINT(0), Value::BIGINT(2), Value(TestFixture::path_multi_component_graph)});

    INFO("Path graph: " + TestFixture::path_multi_component_graph);

    named_parameter_map_t named_parameters({
        {"src", Value("Person")},
        {"type", Value("knows")},
        {"dst", Value("Person")}
    });
    vector<LogicalType> input_table_types({});
    auto bind_input = TestFixture::CreateMockBindInput(inputs, named_parameters, input_table_types);

    vector<LogicalType> return_types;
    vector<std::string> names;
    INFO("Finish mocking");

    TableFunction shortest_path_func = ShortestPath::GetFunction();

    INFO("Bind test");
    TestFixture::conn.BeginTransaction();
    unique_ptr<FunctionData> bind_data;
    REQUIRE_NOTHROW(bind_data = shortest_path_func.bind(*TestFixture::conn.context, bind_input, return_types, names));

    REQUIRE(bind_data != nullptr);
    REQUIRE(names == vector<std::string>({"step_number", "_graphArVertexIndex"}));
    REQUIRE(return_types == vector<LogicalType>({LogicalType::BIGINT, LogicalType::BIGINT}));
    INFO("Finish bind test");

    TableFunctionInitInput func_init_input(bind_data.get(), vector<column_t>(), {}, nullptr);
    INFO("Prepare func_init_input");

    unique_ptr<GlobalTableFunctionState> gstate;
    REQUIRE_NOTHROW(gstate = shortest_path_func.init_global(*TestFixture::conn.context, func_init_input));
    INFO("Finish init global state");

    TableFunctionInput func_input(bind_data.get(), nullptr, gstate);
    INFO("Prepare func_input");

    DataChunk res;
    res.Initialize(*TestFixture::conn.context, return_types);
    DataChunk tmp;
    tmp.Initialize(*TestFixture::conn.context, return_types);

    INFO("Execute test");
    REQUIRE_NOTHROW(shortest_path_func.function(*TestFixture::conn.context, func_input, tmp));
    while (tmp.size() > 0){
        res.Append(tmp, VectorAppendMode::ALLOW_RESIZE);
        tmp.Reset();
        REQUIRE_NOTHROW(shortest_path_func.function(*TestFixture::conn.context, func_input, tmp));
    }
    if(tmp.size() > 0) res.Append(tmp, VectorAppendMode::ALLOW_RESIZE);

    INFO("Checking results");
    REQUIRE(res.size() == 3); // step 0: vertex 0, step 1: vertex 1, step 2: vertex 2
    REQUIRE(res.ColumnCount() == 2);
    
    auto step_data = FlatVector::GetData<int64_t>(res.data[0]);
    auto vertex_data = FlatVector::GetData<int64_t>(res.data[1]);
    
    REQUIRE(step_data[0] == 0);
    REQUIRE(vertex_data[0] == 0);
    REQUIRE(step_data[1] == 1);
    REQUIRE(vertex_data[1] == 1);
    REQUIRE(step_data[2] == 2);
    REQUIRE(vertex_data[2] == 2);
    INFO("Finish execute test");
}

TEMPLATE_TEST_CASE_METHOD(TableFunctionsFixture, "ShortestPath Bind and Execute - non-existent vertex (start == end)", "[shortest_path]", FILE_TYPES_FOR_TEST) {
    INFO("Start mocking data for bind");
    vector<Value> inputs({Value::BIGINT(999999), Value::BIGINT(999999), Value(TestFixture::path_trial_graph)});

    INFO("Path graph: " + TestFixture::path_trial_graph);

    named_parameter_map_t named_parameters({
        {"src", Value("Person")},
        {"type", Value("knows")},
        {"dst", Value("Person")}
    });
    vector<LogicalType> input_table_types({});
    auto bind_input = TestFixture::CreateMockBindInput(inputs, named_parameters, input_table_types);

    vector<LogicalType> return_types;
    vector<std::string> names;
    INFO("Finish mocking");

    TableFunction shortest_path_func = ShortestPath::GetFunction();

    INFO("Bind test");
    TestFixture::conn.BeginTransaction();
    unique_ptr<FunctionData> bind_data;
    REQUIRE_NOTHROW(bind_data = shortest_path_func.bind(*TestFixture::conn.context, bind_input, return_types, names));

    REQUIRE(bind_data != nullptr);
    REQUIRE(names == vector<std::string>({"step_number", "_graphArVertexIndex"}));
    REQUIRE(return_types == vector<LogicalType>({LogicalType::BIGINT, LogicalType::BIGINT}));
    INFO("Finish bind test");

    TableFunctionInitInput func_init_input(bind_data.get(), vector<column_t>(), {}, nullptr);
    INFO("Prepare func_init_input");

    unique_ptr<GlobalTableFunctionState> gstate;
    REQUIRE_NOTHROW(gstate = shortest_path_func.init_global(*TestFixture::conn.context, func_init_input));
    INFO("Finish init global state");

    TableFunctionInput func_input(bind_data.get(), nullptr, gstate);
    INFO("Prepare func_input");

    DataChunk res;
    res.Initialize(*TestFixture::conn.context, return_types);
    DataChunk tmp;
    tmp.Initialize(*TestFixture::conn.context, return_types);

    INFO("Execute test");
    REQUIRE_NOTHROW(shortest_path_func.function(*TestFixture::conn.context, func_input, tmp));
    while (tmp.size() > 0){
        res.Append(tmp, VectorAppendMode::ALLOW_RESIZE);
        tmp.Reset();
        REQUIRE_NOTHROW(shortest_path_func.function(*TestFixture::conn.context, func_input, tmp));
    }
    if(tmp.size() > 0) res.Append(tmp, VectorAppendMode::ALLOW_RESIZE);

    INFO("Checking results");
    REQUIRE(res.size() == 0);
    INFO("Finish execute test");
}

TEMPLATE_TEST_CASE_METHOD(TableFunctionsFixture, "ShortestPath Bind and Execute - non-existent vertex (start > vertex_count)", "[shortest_path]", FILE_TYPES_FOR_TEST) {
    INFO("Start mocking data for bind");
    vector<Value> inputs({Value::BIGINT(999999), Value::BIGINT(1), Value(TestFixture::path_trial_graph)});

    INFO("Path graph: " + TestFixture::path_trial_graph);

    named_parameter_map_t named_parameters({
        {"src", Value("Person")},
        {"type", Value("knows")},
        {"dst", Value("Person")}
    });
    vector<LogicalType> input_table_types({});
    auto bind_input = TestFixture::CreateMockBindInput(inputs, named_parameters, input_table_types);

    vector<LogicalType> return_types;
    vector<std::string> names;
    INFO("Finish mocking");

    TableFunction shortest_path_func = ShortestPath::GetFunction();

    INFO("Bind test");
    TestFixture::conn.BeginTransaction();
    unique_ptr<FunctionData> bind_data;
    REQUIRE_NOTHROW(bind_data = shortest_path_func.bind(*TestFixture::conn.context, bind_input, return_types, names));

    REQUIRE(bind_data != nullptr);
    REQUIRE(names == vector<std::string>({"step_number", "_graphArVertexIndex"}));
    REQUIRE(return_types == vector<LogicalType>({LogicalType::BIGINT, LogicalType::BIGINT}));
    INFO("Finish bind test");

    TableFunctionInitInput func_init_input(bind_data.get(), vector<column_t>(), {}, nullptr);
    INFO("Prepare func_init_input");

    unique_ptr<GlobalTableFunctionState> gstate;
    REQUIRE_NOTHROW(gstate = shortest_path_func.init_global(*TestFixture::conn.context, func_init_input));
    INFO("Finish init global state");

    TableFunctionInput func_input(bind_data.get(), nullptr, gstate);
    INFO("Prepare func_input");

    DataChunk res;
    res.Initialize(*TestFixture::conn.context, return_types);
    DataChunk tmp;
    tmp.Initialize(*TestFixture::conn.context, return_types);

    INFO("Execute test");
    REQUIRE_NOTHROW(shortest_path_func.function(*TestFixture::conn.context, func_input, tmp));
    while (tmp.size() > 0){
        res.Append(tmp, VectorAppendMode::ALLOW_RESIZE);
        tmp.Reset();
        REQUIRE_NOTHROW(shortest_path_func.function(*TestFixture::conn.context, func_input, tmp));
    }
    if(tmp.size() > 0) res.Append(tmp, VectorAppendMode::ALLOW_RESIZE);

    INFO("Checking results");
    REQUIRE(res.size() == 0);
    INFO("Finish execute test");
}

TEMPLATE_TEST_CASE_METHOD(TableFunctionsFixture, "ShortestPath Bind and Execute - non-existent vertex (end > vertex_count)", "[shortest_path]", FILE_TYPES_FOR_TEST) {
    INFO("Start mocking data for bind");
    vector<Value> inputs({Value::BIGINT(1), Value::BIGINT(999999), Value(TestFixture::path_trial_graph)});

    INFO("Path graph: " + TestFixture::path_trial_graph);

    named_parameter_map_t named_parameters({
        {"src", Value("Person")},
        {"type", Value("knows")},
        {"dst", Value("Person")}
    });
    vector<LogicalType> input_table_types({});
    auto bind_input = TestFixture::CreateMockBindInput(inputs, named_parameters, input_table_types);

    vector<LogicalType> return_types;
    vector<std::string> names;
    INFO("Finish mocking");

    TableFunction shortest_path_func = ShortestPath::GetFunction();

    INFO("Bind test");
    TestFixture::conn.BeginTransaction();
    unique_ptr<FunctionData> bind_data;
    REQUIRE_NOTHROW(bind_data = shortest_path_func.bind(*TestFixture::conn.context, bind_input, return_types, names));

    REQUIRE(bind_data != nullptr);
    REQUIRE(names == vector<std::string>({"step_number", "_graphArVertexIndex"}));
    REQUIRE(return_types == vector<LogicalType>({LogicalType::BIGINT, LogicalType::BIGINT}));
    INFO("Finish bind test");

    TableFunctionInitInput func_init_input(bind_data.get(), vector<column_t>(), {}, nullptr);
    INFO("Prepare func_init_input");

    unique_ptr<GlobalTableFunctionState> gstate;
    REQUIRE_NOTHROW(gstate = shortest_path_func.init_global(*TestFixture::conn.context, func_init_input));
    INFO("Finish init global state");

    TableFunctionInput func_input(bind_data.get(), nullptr, gstate);
    INFO("Prepare func_input");

    DataChunk res;
    res.Initialize(*TestFixture::conn.context, return_types);
    DataChunk tmp;
    tmp.Initialize(*TestFixture::conn.context, return_types);

    INFO("Execute test");
    REQUIRE_NOTHROW(shortest_path_func.function(*TestFixture::conn.context, func_input, tmp));
    while (tmp.size() > 0){
        res.Append(tmp, VectorAppendMode::ALLOW_RESIZE);
        tmp.Reset();
        REQUIRE_NOTHROW(shortest_path_func.function(*TestFixture::conn.context, func_input, tmp));
    }
    if(tmp.size() > 0) res.Append(tmp, VectorAppendMode::ALLOW_RESIZE);

    INFO("Checking results");
    REQUIRE(res.size() == 0);
    INFO("Finish execute test");
}
