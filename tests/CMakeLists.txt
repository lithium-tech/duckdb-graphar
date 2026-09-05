# Unit tests for the duckdb_graphar extension.
#
# These tests are built into a standalone Catch2 binary `unittest_graphar`.
# They link against duckdb_static (not the shared libduckdb.so) plus the
# generated extension loader and the extension's static libraries, so they do
# not depend on extension-loader symbols being exported from the shared library.
#
# Paths (ARROW_INSTALL_DIR / GRAPHAR_INSTALL_DIR / GRAPHAR_SOURCE_DIR and the
# duckdb source dir) are provided by the parent CMakeLists when this directory
# is added via add_subdirectory(tests tests).

include(FetchContent)
FetchContent_Declare(
    Catch2
    GIT_REPOSITORY https://github.com/catchorg/Catch2.git
    GIT_TAG        v3.5.2 # The stable version of catch2
)
FetchContent_MakeAvailable(Catch2)

get_target_property(CATCH2_INCLUDE_DIRS Catch2::Catch2 INTERFACE_INCLUDE_DIRECTORIES)

# --- Include paths ----------------------------------------------------------
# DuckDB public headers. duckdb is the top-level project in this build
# (cmake -S ./duckdb/), so CMAKE_SOURCE_DIR points at the duckdb tree.
set(DUCKDB_INCLUDES "${CMAKE_SOURCE_DIR}/src/include")

# Extension public headers + Arrow/GraphAr installed headers + GraphAr's bundled
# third-party headers (mini-yaml etc.), as the extension itself is built with.
set(EXTENSION_INCLUDES
    $<BUILD_INTERFACE:${EXTENSION_ROOT_DIR}/include>
    $<BUILD_INTERFACE:${GRAPHAR_INSTALL_DIR}/include>
    $<BUILD_INTERFACE:${GRAPHAR_SOURCE_DIR}/cpp/thirdparty>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/src/include>
    $<BUILD_INTERFACE:${ARROW_INSTALL_DIR}/include>
)

add_subdirectory(table_functions)
add_subdirectory(scalar_functions)

add_executable(unittest_graphar ${ALL_OBJECT_FILES})

target_link_libraries(unittest_graphar
    PRIVATE
        duckdb_static
        duckdb_generated_extension_loader
        core_functions_extension
        parquet_extension
        duckdb_graphar_extension
        graphar
        Arrow::arrow_static
        Catch2::Catch2WithMain
)

target_include_directories(unittest_graphar PRIVATE
    ${DUCKDB_INCLUDES}
    ${EXTENSION_INCLUDES}
)

# Integrate with ctest for easy test execution.
add_test(NAME ${DUCKDB_EXTENSION_NAME}_unittest COMMAND unittest_graphar)