log_stage("tablefunctions")
add_library_unity(test_table_functions OBJECT test_read_vertices.cpp test_read_edges.cpp test_hop.cpp)
add_dependencies(test_table_functions graphar)
target_include_directories(test_table_functions PRIVATE
    ${GRAPHAR_INSTALL_DIR}/include
    ${ARROW_INSTALL_DIR}/include
    ${EXTENSION_ROOT_DIR}/include
    ${CATCH2_INCLUDE_DIRS}
    ${EXTENSION_ROOT_DIR}/tests/fixture
)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_table_functions>
    PARENT_SCOPE)
