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
# Statically link the extension into the duckdb shell and unittest so it is
# auto-registered instead of requiring a separate LOAD. (DuckDB v2.0.0+ only
# links extensions listed here; duckdb_extension_load alone builds loadable only.)
duckdb_extension_statically_link(duckdb_graphar)
