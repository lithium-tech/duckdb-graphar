# Configuration for the DuckDB 'graphar_duck' extension.
#

duckdb_extension_load(graphar_duck
    SOURCE_DIR "${EXTENSION_ROOT_DIR}/config"
    INCLUDE_DIR "${EXTENSION_ROOT_DIR}/include"
    LINKED_LIBS "arrow;graphar"
)
