cmake_minimum_required(VERSION 3.20)

project(duckdb_graphar)
set(EXTENSION_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR} CACHE STRING "Extension root directory")

# =============================================
# Core Configuration
# =============================================
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

include(FetchContent)
include(GNUInstallDirs)
include(ExternalProject)

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
set(DEPS_INSTALL_DIR ${CMAKE_BINARY_DIR}/_deps)
set(ARROW_INSTALL_DIR ${DEPS_INSTALL_DIR}/arrow-install)
set(GRAPHAR_INSTALL_DIR ${DEPS_INSTALL_DIR}/graphar-install)

set(ARROW_PREFIX_DIR ${DEPS_INSTALL_DIR}/arrow-prefix)
set(GRAPHAR_PREFIX_DIR ${DEPS_INSTALL_DIR}/graphar-prefix)

set(ARROW_LIB_DIR ${ARROW_INSTALL_DIR}/lib)
set(GRAPHAR_LIB_DIR ${GRAPHAR_INSTALL_DIR}/lib)

# Arrow
log_stage("Configuring Apache Arrow...")

ExternalProject_Add(
  arrow
  PREFIX ${ARROW_PREFIX_DIR}
  GIT_REPOSITORY https://github.com/apache/arrow.git
  GIT_TAG apache-arrow-17.0.0
  SOURCE_SUBDIR cpp
  CMAKE_ARGS
    -DCMAKE_POLICY_VERSION_MINIMUM=3.5
    -DARROW_BUILD_SHARED=ON
    -DARROW_BUILD_STATIC=OFF
    -DARROW_BUILD_TESTS=OFF
    -DARROW_BUILD_BENCHMARKS=OFF
    -DARROW_BUILD_EXAMPLES=OFF
    -DARROW_DEPENDENCY_SOURCE=BUNDLED
    -DARROW_OPTIONAL_INSTALL=ON
    -DARROW_COMPUTE=ON
    -DARROW_DATASET=ON
    -DARROW_FILESYSTEM=ON
    -DARROW_S3=ON
    -DARROW_PARQUET=ON
    -DARROW_ORC=ON
    -DARROW_JSON=ON
    -DARROW_WITH_ZLIB=ON
    -DARROW_WITH_ZSTD=ON
    -DARROW_WITH_BROTLI=OFF
    -DARROW_WITH_BZ2=OFF
    -DARROW_WITH_LZ4=OFF
    -DARROW_WITH_SNAPPY=ON
    -DARROW_CSV=ON
    -DARROW_GANDIVA=OFF
    -DARROW_TESTING=OFF
    -DCMAKE_INSTALL_PREFIX=${ARROW_INSTALL_DIR}
  BUILD_ALWAYS OFF
  INSTALL_DIR ${ARROW_INSTALL_DIR}
)

add_library(arrow::arrow_shared SHARED IMPORTED GLOBAL)

file(MAKE_DIRECTORY "${ARROW_INSTALL_DIR}/include")
set_target_properties(arrow::arrow_shared PROPERTIES
  IMPORTED_LOCATION "${ARROW_LIB_DIR}/libarrow${CMAKE_SHARED_LIBRARY_SUFFIX}"
  INTERFACE_INCLUDE_DIRECTORIES ${ARROW_INSTALL_DIR}/include
)

add_dependencies(arrow::arrow_shared arrow)

# GraphAr

log_stage("Setting up GraphAR...")

ExternalProject_Add(
  graphar
  PREFIX ${GRAPHAR_PREFIX_DIR}
  GIT_REPOSITORY https://github.com/apache/incubator-graphar.git
  # GIT_TAG main
  GIT_TAG 1a44bc343e0e846df129c78152f6310a1d678429
  SOURCE_SUBDIR cpp
  CMAKE_ARGS
    -DCMAKE_INSTALL_PREFIX=${GRAPHAR_INSTALL_DIR}
    -DCMAKE_PREFIX_PATH=${ARROW_INSTALL_DIR}
    -DProtobuf_INCLUDE_DIR=${ARROW_PREFIX_DIR}/src/arrow-build/protobuf_ep-install/include
    -DProtobuf_LIBRARIES=${ARROW_PREFIX_DIR}/src/arrow-build/protobuf_ep-install/lib/libprotobuf.a
  INSTALL_DIR ${GRAPHAR_INSTALL_DIR}
  DEPENDS arrow
)

add_library(graphar::graphar_shared SHARED IMPORTED GLOBAL)

set_target_properties(graphar::graphar_shared PROPERTIES
  IMPORTED_LOCATION "${GRAPHAR_LIB_DIR}/libgraphar${CMAKE_SHARED_LIBRARY_SUFFIX}"
)

add_dependencies(graphar::graphar_shared graphar arrow)

# Set paths for shared libraries
set(CMAKE_BUILD_RPATH
    "@loader_path"
    "${GRAPHAR_LIB_DIR}"
    "${ARROW_LIB_DIR}"
)

set(CMAKE_INSTALL_RPATH
    "@loader_path"
    "${GRAPHAR_LIB_DIR}"
    "${ARROW_LIB_DIR}"
)

set(CMAKE_SKIP_BUILD_RPATH FALSE)
set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)

log_done()

# DuckDB
set(DUCKDB_EXTENSION_CONFIGS "${CMAKE_CURRENT_SOURCE_DIR}/config/extension_config.cmake")

log_stage("Declaring DuckDB")
#set(BUILD_UNITTESTS OFF CACHE BOOL "")
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

log_stage("Building DuckDB")
FetchContent_MakeAvailable(duckdb)
log_done()

# Final build steps (DuckDB + extensions) is configured in config/CMakeLists.txt
