cmake_minimum_required(VERSION 3.20)
project(graphar_duck)
string(TOUPPER ${PROJECT_NAME} PROJECT_NAME_UPPERCASE)
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

option(BUILD_LOADABLE_EXTENSION "Build as loadable extension" ON)
option(LOAD_TESTS "Build extension tests" OFF)
option(SHOW_LOG_STAGES "Display progress logs for each stage" ON)

# # =============================================
# # Progress Reporting Functions
# # =============================================
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
# DuckDB
set(DUCKDB_EXTENSION_CONFIGS "${CMAKE_CURRENT_SOURCE_DIR}/config/extension_config.cmake")

log_stage("Declaring DuckDB")
set(BUILD_UNITTESTS OFF CACHE BOOL "")
set(ENABLE_SANITIZER OFF CACHE BOOL "")
set(ENABLE_UBSAN OFF CACHE BOOL "")
set(DISABLE_VPTR_SANITIZER ON CACHE BOOL "")
set(SKIP_EXTENSIONS "graphar_duck")

FetchContent_Declare(
    duckdb
    GIT_REPOSITORY https://github.com/duckdb/duckdb.git
    GIT_TAG v1.3.2
    GIT_SHALLOW TRUE
    GIT_PROGRESS TRUE
)
log_done()

log_stage("Making available")
FetchContent_MakeAvailable(duckdb)
log_done()

# Arrow
log_stage("Configuring Apache Arrow...")

ExternalProject_Add(
  arrow
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
    -DARROW_COMPUTE=ON
    -DARROW_DATASET=ON
    -DARROW_FILESYSTEM=ON
    -DARROW_PARQUET=ON
    -DARROW_ORC=ON
    -DARROW_WITH_ZLIB=ON
    -DARROW_WITH_ZSTD=ON
    -DARROW_WITH_BROTLI=OFF
    -DARROW_WITH_BZ2=OFF
    -DARROW_WITH_LZ4=OFF
    -DARROW_WITH_SNAPPY=ON
    -DARROW_CSV=ON
    -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/arrow-install
  BUILD_ALWAYS OFF
  INSTALL_DIR ${CMAKE_BINARY_DIR}/arrow-install
)

add_library(arrow::arrow_shared SHARED IMPORTED GLOBAL)

set_target_properties(arrow::arrow_shared PROPERTIES
  IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/arrow-install/lib/libarrow.so
  INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_BINARY_DIR}/arrow-install/include
)

add_dependencies(arrow::arrow_shared arrow)

# GraphAr

set(Protobuf_INCLUDE_DIR "${CMAKE_BINARY_DIR}/duckdb-build/extension/${PROJECT_NAME}/arrow-prefix/src/arrow-build/protobuf_ep-install/include")
set(Protobuf_LIBRARIES "${CMAKE_BINARY_DIR}/duckdb-build/extension/${PROJECT_NAME}/arrow-prefix/src/arrow-build/protobuf_ep-install/lib/libprotobuf.a")

log_stage("Setting up GraphAR...")

ExternalProject_Add(
  graphar
  GIT_REPOSITORY https://github.com/apache/incubator-graphar.git
  GIT_TAG main
  SOURCE_SUBDIR cpp
  CMAKE_ARGS
    -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/graphar-install
    -DCMAKE_PREFIX_PATH=${CMAKE_BINARY_DIR}/arrow-install
    -DARROW_HOME=${CMAKE_BINARY_DIR}/arrow-install
    -DGRAPHAR_BUILD_SHARED=ON
    -DGRAPHAR_BUILD_STATIC=OFF
    -DGRAPHAR_TEST=OFF
  INSTALL_DIR ${CMAKE_BINARY_DIR}/graphar-install
  DEPENDS arrow
)

add_library(graphar::graphar_shared SHARED IMPORTED GLOBAL)

set_target_properties(graphar::graphar_shared PROPERTIES
  IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/graphar-install/lib/libgraphar.so
  # INTERFACE_INCLUDE_DIRECTORIES ${graphar_SOURCE_DIR}/graphar-install/include
  INTERFACE_LINK_LIBRARIES arrow::arrow_shared
)

add_dependencies(graphar::graphar_shared graphar arrow)

log_done()

# =============================================
# Build Targets
# =============================================
file(GLOB_RECURSE EXTENSION_SOURCES CONFIGURE_DEPENDS "${EXTENSION_ROOT_DIR}/src/*.cpp")

log_stage("Building static extension")
build_static_extension(graphar_duck ${EXTENSION_SOURCES})
log_done()

set(EXTENSION_INCLUDES
     $<BUILD_INTERFACE:${EXTENSION_ROOT_DIR}/include>
    $<BUILD_INTERFACE:${graphar_SOURCE_DIR}/cpp/thirdparty>  # GraphAr thirdparty
    $<BUILD_INTERFACE:${graphar_SOURCE_DIR}/cpp/src>  # GraphAr main includes (?)
    $<BUILD_INTERFACE:${graphar_BINARY_DIR}/cpp/src>
     $<BUILD_INTERFACE:${duckdb_SOURCE_DIR}/src/include>  # DuckDB internal (?)
    $<BUILD_INTERFACE:${arrow_SOURCE_DIR}/cpp/src>  # Arrow main includes (?)
    $<BUILD_INTERFACE:${arrow_BINARY_DIR}/cpp/src>  # Generated Arrow headers (?)
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
)
target_include_directories(graphar_duck_extension PUBLIC ${EXTENSION_INCLUDES} $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

target_link_libraries(graphar_duck_extension
    duckdb_static
    arrow::arrow_shared
    graphar::graphar_shared
)

# =============================================
# Installation
# =============================================
set(INSTALL_LIB_DIR "${CMAKE_INSTALL_LIBDIR}" CACHE PATH "Library installation directory")

install(
  TARGETS graphar_duck_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

install(DIRECTORY include/ DESTINATION include
    FILES_MATCHING PATTERN "*.h" PATTERN "*.hpp"
)
