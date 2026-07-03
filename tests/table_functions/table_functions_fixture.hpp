#pragma once
#include "basic_graphar_fixture.hpp"
#include "functions/table/read_edges.hpp"
#include "functions/table/read_vertices.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

template <typename FileTypeTag> 
class TableFunctionsFixture: public BasicGrapharFixture<FileTypeTag> {
protected:
    // Trial graph: single connected component (vertices 1-5)
    std::string path_trial_graph;
    std::string path_edges_trial_graph;
    std::string folder_trial_graph;

    // Multi-component graph: 3 disconnected components
    // Component 1: 1-2-3-4-5 (chain)
    // Component 2: 6-8-10 (chain)
    // Component 3: 9-7 (chain)
    std::string path_multi_component_graph;
    std::string path_edges_multi_component_graph;
    std::string folder_multi_component_graph;

    std::string path_feature_graph;
    std::string path_edges_feature_graph;
    std::string folder_feature_graph;
    
    static duckdb::TableFunctionBindInput CreateMockBindInput(duckdb::vector<duckdb::Value> &inputs, duckdb::named_parameter_map_t &named_parameters, duckdb::vector<duckdb::LogicalType> &input_table_types) {
        duckdb::vector<std::string> input_table_names;
        duckdb::TableFunction table_function;
        duckdb::TableFunctionRef ref;

        return duckdb::TableFunctionBindInput(inputs, named_parameters, input_table_types,
                                    input_table_names, nullptr, nullptr,
                                    table_function, ref);
    }
public:
    ~TableFunctionsFixture() = default;
    TableFunctionsFixture(): BasicGrapharFixture<FileTypeTag>() {
        constexpr const char* VERTEX_LABEL = "Person";
        constexpr const char* EDGE_LABEL = "knows"; 
        constexpr const char* TRIAL_GRAPH_NAME = "trial"; 
        REQUIRE_NOTHROW(
            folder_trial_graph = BasicGrapharFixture<FileTypeTag>::CreateTestGraph(
                TRIAL_GRAPH_NAME, 
                {
                    VerticesSchema(
                        VERTEX_LABEL, 1024, 
                        { PropertySchema("hash_phone_no", "int32", false, true) }, 
                        {
                            {1, {{"hash_phone_no", int32_t{10}}}}, 
                            {2, {{"hash_phone_no", int32_t{20}}}}, 
                            {3, {{"hash_phone_no", int32_t{30}}}}, 
                            {4, {{"hash_phone_no", int32_t{40}}}}, 
                            {5, {{"hash_phone_no", int32_t{50}}}}
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
                            {4, 5}
                        },
                        5
                    )
                }
            )
        );
        
        path_trial_graph = folder_trial_graph + "/" + TRIAL_GRAPH_NAME + GraphFileExtension;
        path_edges_trial_graph = folder_trial_graph + "/" +  VERTEX_LABEL + "_" + EDGE_LABEL + "_" + VERTEX_LABEL+ EdgeFileExtension;

        constexpr const char* FEATURE_GRAPH_NAME = "trial_f"; 
        REQUIRE_NOTHROW(
            folder_feature_graph = BasicGrapharFixture<FileTypeTag>::CreateTestGraph(
                FEATURE_GRAPH_NAME, 
                {
                    VerticesSchema(
                        VERTEX_LABEL, 1024, 
                        {
                            PropertySchema("hash_phone_no", "int32", false, true), 
                            PropertySchema("first_name", "string", false, false),
                            PropertySchema("last_name", "string", false, false)
                        }, 
                        {
                            {1, {{"hash_phone_no", int32_t{10}}, {"first_name", std::string{"Emily"}}, {"last_name", std::string{"Johnson"}}}}, 
                            {2, {{"hash_phone_no", int32_t{20}}, {"first_name", std::string{"James"}}, {"last_name", std::string{"Wilson"}}}}, 
                            {3, {{"hash_phone_no", int32_t{30}}, {"first_name", std::string{"Olivia"}}, {"last_name", std::string{"Brown"}}}}, 
                            {4, {{"hash_phone_no", int32_t{40}}, {"first_name", std::string{"Benjamin"}}, {"last_name", std::string{"Taylor"}}}}, 
                            {5, {{"hash_phone_no", int32_t{50}}, {"first_name", std::string{"Sophia"}}, {"last_name", std::string{"Martinez"}}}}
                        }
                    )
                }, 
                {
                    EdgesSchema(
                        VERTEX_LABEL, EDGE_LABEL, VERTEX_LABEL, 0, false, 
                        {
                            PropertySchema("friend_score", "int32", false, false),
                            PropertySchema("created_at", "string", false, false), 
                            PropertySchema("tmp_", "float", false, false)}, 
                        {
                            {1, 2, {{"friend_score", int32_t{1}}, {"created_at", std::string{"2021-01-01"}}, {"tmp_", float{0.1}}}}, 
                            {1, 3, {{"friend_score", int32_t{2}}, {"created_at", std::string{"2022-01-01"}}, {"tmp_", float{0.1}}}}, 
                            {2, 3, {{"friend_score", int32_t{3}}, {"created_at", std::string{"2021-11-01"}}, {"tmp_", float{0.1}}}}, 
                            {2, 4, {{"friend_score", int32_t{4}}, {"created_at", std::string{"2021-01-01"}}, {"tmp_", float{0.1}}}}, 
                            {3, 4, {{"friend_score", int32_t{1}}, {"created_at", std::string{"2021-01-01"}}, {"tmp_", float{0.1}}}}, 
                            {3, 5, {{"friend_score", int32_t{1}}, {"created_at", std::string{"2021-01-01"}}, {"tmp_", float{0.1}}}}, 
                            {4, 5, {{"friend_score", int32_t{1}}, {"created_at", std::string{"2021-01-01"}}, {"tmp_", float{0.1}}}}
                        },
                        5
                    )
                }
            )
        );
        path_feature_graph = folder_feature_graph + "/" + FEATURE_GRAPH_NAME + GraphFileExtension;
        path_edges_feature_graph = folder_feature_graph + "/" +  VERTEX_LABEL + "_" + EDGE_LABEL + "_" + VERTEX_LABEL + EdgeFileExtension;

        // Multi-component graph: 3 disconnected components
        // Component 1: vertices 0-4 (chain: 0->1->2->3->4)
        // Component 2: vertices 5,7,9 (chain: 5->7->9)
        // Component 3: vertices 6,8 (chain: 8->6)
        constexpr const char* MULTI_COMPONENT_GRAPH_NAME = "multi_component";
        REQUIRE_NOTHROW(
            folder_multi_component_graph = BasicGrapharFixture<FileTypeTag>::CreateTestGraph(
                MULTI_COMPONENT_GRAPH_NAME,
                {
                    VerticesSchema(
                        VERTEX_LABEL, 1024,
                        { PropertySchema("hash_phone_no", "int32", false, true) },
                        {
                            {0, {{"hash_phone_no", int32_t{0}}}},
                            {1, {{"hash_phone_no", int32_t{10}}}},
                            {2, {{"hash_phone_no", int32_t{20}}}},
                            {3, {{"hash_phone_no", int32_t{30}}}},
                            {4, {{"hash_phone_no", int32_t{40}}}},
                            {5, {{"hash_phone_no", int32_t{50}}}},
                            {6, {{"hash_phone_no", int32_t{60}}}},
                            {7, {{"hash_phone_no", int32_t{70}}}},
                            {8, {{"hash_phone_no", int32_t{80}}}},
                            {9, {{"hash_phone_no", int32_t{90}}}}
                        }
                    )
                },
                {
                    EdgesSchema(
                        VERTEX_LABEL, EDGE_LABEL, VERTEX_LABEL, 0, false,
                        {},
                        {
                            // Component 1: 0->1->2->3->4
                            {0, 1},
                            {1, 2},
                            {2, 3},
                            {3, 4},
                            // Component 2: 5->7->9
                            {5, 7},
                            {7, 9},
                            // Component 3: 8->6
                            {8, 6}
                        },
                        10  // num_vertices
                    )
                }
            )
        );
        path_multi_component_graph = folder_multi_component_graph + "/" + MULTI_COMPONENT_GRAPH_NAME + GraphFileExtension;
        path_edges_multi_component_graph = folder_multi_component_graph + "/" + VERTEX_LABEL + "_" + EDGE_LABEL + "_" + VERTEX_LABEL + EdgeFileExtension;
    };
};
