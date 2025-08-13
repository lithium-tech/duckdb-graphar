cmake_minimum_required(VERSION 3.20)
project(duckdb_graphar
    VERSION 0.9.0
    LANGUAGES CXX
    DESCRIPTION "DuckDB extension for Apache GraphAr integration"
)

# =============================================
# Core Configuration
# =============================================
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

include(FetchContent)
include(GNUInstallDirs)

option(BUILD_LOADABLE_EXTENSION "Build as loadable extension" ON)
option(LOAD_TESTS "Build extension tests" OFF)
option(SHOW_LOG_STAGES "Display progress logs for each stage" ON)

# =============================================
# Progress Reporting Functions
# =============================================
function(log_stage MESSAGE)
    if(SHOW_LOG_STAGES)
        message(STATUS ">>> [${PROJECT_NAME}] ${MESSAGE}")
    endif()
endfunction()

function(log_done)
    if(SHOW_LOG_STAGES)
        message(STATUS ">>> [${PROJECT_NAME}] Done.")
    endif()
endfunction()

# =============================================
# Dependency Management
# =============================================
# Is 'extension-ci-tools' needed?
#log_stage("Setting up extension-ci-tools...")
#
#FetchContent_Declare(
#    extension-ci-tools
#    GIT_REPOSITORY https://github.com/duckdb/extension-ci-tools.git
#    GIT_TAG main
#    GIT_SHALLOW TRUE
#    GIT_PROGRESS TRUE
#)
#
#FetchContent_MakeAvailable(extension-ci-tools)
#
#log_done()

log_stage("Configuring DuckDB...")

set(BUILD_UNITTESTS OFF CACHE BOOL "")
set(ENABLE_SANITIZER OFF CACHE BOOL "")
set(ENABLE_UBSAN OFF CACHE BOOL "")
set(DISABLE_VPTR_SANITIZER ON CACHE BOOL "")

FetchContent_Declare(
    duckdb
    GIT_REPOSITORY https://github.com/duckdb/duckdb.git
    GIT_TAG v1.3.2
    GIT_SHALLOW TRUE
    GIT_PROGRESS TRUE
)

log_done()

log_stage("Configuring Apache Arrow...")

set(ARROW_BUILD_SHARED ON CACHE BOOL "")
set(ARROW_BUILD_STATIC OFF CACHE BOOL "")
set(ARROW_BUILD_TESTS OFF CACHE BOOL "")
set(ARROW_BUILD_BENCHMARKS OFF CACHE BOOL "")
set(ARROW_BUILD_EXAMPLES OFF CACHE BOOL "")
set(ARROW_DEPENDENCY_SOURCE "BUNDLED" CACHE STRING "")

set(ARROW_COMPUTE ON CACHE BOOL "")
set(ARROW_DATASET ON CACHE BOOL "")
set(ARROW_FILESYSTEM ON CACHE BOOL "")
set(ARROW_PARQUET ON CACHE BOOL "")
set(ARROW_ORC ON CACHE BOOL "")
set(ARROW_WITH_ZLIB ON CACHE BOOL "")
set(ARROW_WITH_ZSTD ON CACHE BOOL "")
set(ARROW_WITH_SNAPPY ON CACHE BOOL "")

FetchContent_Declare(
    arrow
    GIT_REPOSITORY https://github.com/apache/arrow.git
    GIT_TAG apache-arrow-17.0.0
    GIT_SHALLOW TRUE
    GIT_PROGRESS TRUE
)

log_done()

log_stage("Setting up GraphAR...")

set(BUILD_TESTS OFF CACHE BOOL "")
set(BUILD_EXAMPLES OFF CACHE BOOL "")

FetchContent_Declare(
    graphar
    GIT_REPOSITORY https://github.com/apache/incubator-graphar.git
    GIT_TAG main
    # GIT_COMMIT 1cca4035f271fd117406d1d555a5ce1d3cd7ad51
    GIT_SHALLOW TRUE
    GIT_PROGRESS TRUE
)

log_done()

# Make all dependencies available
FetchContent_MakeAvailable(duckdb arrow graphar)

log_stage("Configuring extension build...")

# =============================================
# Build Targets
# =============================================

set(EXTENSION_INCLUDES
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${graphar_SOURCE_DIR}/cpp/thirdparty  # GraphAr thirdparty
    ${graphar_SOURCE_DIR}/cpp/include  # GraphAr main includes (?)
    ${duckdb_SOURCE_DIR}/src/include  # DuckDB internal (?)
    ${arrow_SOURCE_DIR}/cpp/src  # Arrow main includes (?)
    ${arrow_BINARY_DIR}/cpp/src  # Generated Arrow headers (?)

file(GLOB_RECURSE EXTENSION_SOURCES CONFIGURE_DEPENDS "src/*.cpp")

add_library(duckdb_graphar_extension STATIC ${EXTENSION_SOURCES})
target_include_directories(duckdb_graphar_extension PUBLIC ${EXTENSION_INCLUDES})
target_link_libraries(duckdb_graphar_extension PUBLIC
    duckdb_static
    Arrow::arrow_shared
    graphar
)

# =============================================
# Integration with DuckDB Binary
# =============================================

# Add extension to DuckDB's build
set(DUCKDB_EXTENSION_NAMES ${PROJECT_NAME} CACHE STRING "" FORCE)
set(DUCKDB_EXTENSION_${PROJECT_NAME}_SOURCES ${EXTENSION_SOURCES} CACHE STRING "" FORCE)
set(DUCKDB_EXTENSION_${PROJECT_NAME}_LINK_LIBRARIES "Arrow::arrow_shared;graphar" CACHE STRING "" FORCE)

# =============================================
# Installation
# =============================================
set(INSTALL_LIB_DIR "${CMAKE_INSTALL_LIBDIR}" CACHE PATH "Library installation directory")

install(TARGETS duckdb_graphar_extension
    LIBRARY DESTINATION ${INSTALL_LIB_DIR}
    ARCHIVE DESTINATION ${INSTALL_LIB_DIR}
)

if(BUILD_LOADABLE_EXTENSION)
    install(TARGETS duckdb_graphar
        LIBRARY DESTINATION ${INSTALL_LIB_DIR}
    )
endif()

install(DIRECTORY include/ DESTINATION include
    FILES_MATCHING PATTERN "*.h" PATTERN "*.hpp"
)
