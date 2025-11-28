#pragma once
#include "basic_graphar_fixture.hpp"

struct ExpressionStateWithDeps {
    std::shared_ptr<duckdb::BoundFunctionExpression> bound_expr;
    std::shared_ptr<duckdb::ExpressionExecutorState> executor_state;
    std::shared_ptr<duckdb::ExpressionExecutor> executor;
    duckdb::ExpressionState state;
    
    ExpressionStateWithDeps(std::shared_ptr<duckdb::BoundFunctionExpression> expr,
                        std::shared_ptr<duckdb::ExpressionExecutorState> exec_state, duckdb::ClientContext &context)
        : bound_expr(std::move(expr)), executor_state(std::move(exec_state)), 
        executor(std::make_shared<duckdb::ExpressionExecutor>(context)), state(*bound_expr, *executor_state) {
            executor_state->executor = executor.get();
        }
};
template <typename FileTypeTag> 
class ScalarFunctionsFixture: public BasicGrapharFixture<FileTypeTag> {
public:
    std::string path_trial_graph;
    std::string path_large_graph;

    ~ScalarFunctionsFixture() = default;
    ScalarFunctionsFixture(): BasicGrapharFixture<FileTypeTag>() {
        constexpr const char* VERTEX_LABEL = "Person";
        constexpr const char* EDGE_LABEL = "knows"; 

        constexpr const char* TRIAL_GRAPH_NAME = "trial";
        std::string folder_trial_graph;
        REQUIRE_NOTHROW(
            folder_trial_graph = BasicGrapharFixture<FileTypeTag>::CreateTestGraph(
                TRIAL_GRAPH_NAME, 
                {
                    VerticesSchema(
                        VERTEX_LABEL, 1024, 
                        {PropertySchema("hash_phone_no", "int32", false, true)}, 
                        {
                            {1, {{"hash_phone_no", int32_t{10}}}}, 
                            {2, {{"hash_phone_no", int32_t{20}}}}, 
                            {3, {{"hash_phone_no", int32_t{30}}}}, 
                            {4, {{"hash_phone_no", int32_t{40}}}}, 
                            {5, {{"hash_phone_no", int32_t{50}}}},
                            {6, {{"hash_phone_no", int32_t{60}}}},
                            {7, {{"hash_phone_no", int32_t{70}}}},
                            {8, {{"hash_phone_no", int32_t{80}}}},
                            {9, {{"hash_phone_no", int32_t{90}}}},
                            {10, {{"hash_phone_no", int32_t{100}}}}
                        }
                    )
                }, 
                {
                    EdgesSchema(
                        VERTEX_LABEL, EDGE_LABEL, VERTEX_LABEL, 0, false, 
                        {}, 
                        {
                            {1, 2}, 
                            {1, 3}, 
                            {2, 3}, 
                            {2, 4}, 
                            {3, 4}, 
                            {3, 5}, 
                            {4, 5},
                            {6, 7},
                            {6, 8},
                            {8, 10},
                            {10, 6},
                            {9, 7}
                        },
                        10
                    )
                }
            )
        );
        REQUIRE(!folder_trial_graph.empty());
        path_trial_graph = folder_trial_graph + "/" + TRIAL_GRAPH_NAME + GraphFileExtension;

        std::vector<VertexData> vertices(530);
        std::vector<EdgeData> edges(556);
        vertices[0] = {0, {{"hash_phone_no", int32_t{0}}, {"first_name", std::string{"Person"}}, {"last_name", std::string{"no_"} + std::to_string(0)}}};
        vertices[30] =  {30, {{"hash_phone_no", int32_t{300}}, {"first_name", std::string{"Person"}}, {"last_name", std::string{"no "} + std::to_string(30)}}};;
        vertices[529] =  {529, {{"hash_phone_no", int32_t{5290}}, {"first_name", std::string{"Person"}}, {"last_name", std::string{"no "} + std::to_string(529)}}};;
        for (int i = 1; i < 30; ++i) {
            edges[2*(i-1)] = {0, i, {{"friend_score", int32_t{1}}, {"created_at", std::string{"2021-01-01"}}, {"tmp_", float{0.1}}}};
            edges[2*(i-1)+1] = {i, 30, {{"friend_score", int32_t{2}}, {"created_at", std::string{"2021-11-01"}}, {"tmp_", float{0.1}}}};
            vertices[i] =  {i, {{"hash_phone_no", int32_t{i * 10}}, {"first_name", std::string{"Person"}}, {"last_name", std::string{"no "} + std::to_string(i)}}};
        }
        for (int i = 31; i <= 528; ++i) {
            edges[27 + i] = {i-1, i, {{"friend_score", int32_t{2}}, {"created_at", std::string{"2022-01-01"}}, {"tmp_", float{0.1}}}}, 
            vertices[i] = {i, {{"hash_phone_no", int32_t{i * 10}}, {"first_name", std::string{"Person"}}, {"last_name", std::string{"no "} + std::to_string(i)}}};
        }
        constexpr const char* LARGE_GRAPH_NAME = "large_graph";
        std::string folder_large_graph;
        REQUIRE_NOTHROW(
            folder_large_graph = BasicGrapharFixture<FileTypeTag>::CreateTestGraph(
                LARGE_GRAPH_NAME, 
                {
                    VerticesSchema(
                        VERTEX_LABEL, 1024, 
                        {
                            PropertySchema("hash_phone_no", "int32", false, true), 
                            PropertySchema("first_name", "string", false, false),
                            PropertySchema("last_name", "string", false, false)
                        }, 
                        vertices
                    )
                }, 
                {
                    EdgesSchema(
                        VERTEX_LABEL, EDGE_LABEL, VERTEX_LABEL, 0, false, 
                        {
                            PropertySchema("friend_score", "int32", false, false),
                            PropertySchema("created_at", "string", false, false), 
                            PropertySchema("tmp_", "float", false, false)
                        }, 
                        edges,
                        vertices.size()
                    )
                }
            )
        );
        REQUIRE(!folder_large_graph.empty());
        path_large_graph = folder_large_graph + "/" + LARGE_GRAPH_NAME + GraphFileExtension;   
    };
    
    static ExpressionStateWithDeps MockingState(
        duckdb::LogicalType return_type,
        duckdb::ScalarFunction func,
        duckdb::ClientContext &context
        
    ) {
        auto bound_expr = std::make_shared<duckdb::BoundFunctionExpression>(return_type, func, std::vector<duckdb::unique_ptr<duckdb::Expression>>{}, nullptr, false);
        REQUIRE(bound_expr != nullptr);
        auto executor_state = std::make_shared<duckdb::ExpressionExecutorState>();

        return ExpressionStateWithDeps(bound_expr, executor_state, context);
    }
};
