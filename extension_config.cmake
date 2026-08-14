# Configuration for the DuckDB 'duckdb_graphar' extension.
#
if(NOT DEFINED EXTENSION_ROOT_DIR)
    set(LOAD_INCLUDE_DIR "${CMAKE_CURRENT_LIST_DIR}")
else()
    set(LOAD_INCLUDE_DIR "${EXTENSION_ROOT_DIR}")
endif()

duckdb_extension_load(duckdb_graphar
    INCLUDE_DIR "${LOAD_INCLUDE_DIR}/include"
    SOURCE_DIR "${CMAKE_CURRENT_LIST_DIR}"
    LOAD_TESTS
)
