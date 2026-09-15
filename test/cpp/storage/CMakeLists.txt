# Storage tests for the duckdb_graphar extension.
add_library_unity(test_storage OBJECT test_graphar_storage.cpp test_graphar_schema_entry.cpp)
add_dependencies(test_storage graphar)
target_include_directories(test_storage PRIVATE
    ${GRAPHAR_INSTALL_DIR}/include
    ${GRAPHAR_SOURCE_DIR}/cpp/thirdparty
    ${ARROW_INSTALL_DIR}/include
    ${EXTENSION_ROOT_DIR}/include
    ${EXTENSION_ROOT_DIR}/analytics/usage_analytics/include
    ${Boost_INCLUDE_DIRS}
    ${CATCH2_INCLUDE_DIRS}
    ${EXTENSION_ROOT_DIR}/test/cpp/fixture
)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_storage>
    PARENT_SCOPE)