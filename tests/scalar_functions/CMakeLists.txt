log_stage("scalar_functions")
add_library_unity(test_graphar_scalar_funcs OBJECT test_bfs.cpp)
add_dependencies(test_graphar_scalar_funcs graphar)
target_include_directories(test_graphar_scalar_funcs PRIVATE
    ${GRAPHAR_INSTALL_DIR}/include
    ${ARROW_INSTALL_DIR}/include
    ${EXTENSION_ROOT_DIR}/include
    ${CATCH2_INCLUDE_DIRS}
    ${EXTENSION_ROOT_DIR}/tests/fixture
)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_graphar_scalar_funcs>
    PARENT_SCOPE)
