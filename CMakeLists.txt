cmake_minimum_required(VERSION 3.20)
project(duckdb_graphar
  VERSION 0.9.0
  LANGUAGES CXX
)

# =============================================
# Core Configuration
# =============================================
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

option(BUILD_LOADABLE_EXTENSION "Build as loadable extension" ON)
option(LOAD_TESTS "Build extension tests" OFF)

# =============================================
# Extension Source Files (EXPLICITLY LISTED)
# =============================================
set(EXTENSION_SOURCES
  src/graphar_duck_extension.cpp
  src/utils/func.cpp
  src/utils/benchmark.cpp
  src/functions/table/read_vertices.cpp
  src/functions/table/read_edges.cpp
  src/functions/scalar/bfs.cpp
  src/functions/table/edges_vertex.cpp
  src/functions/table/hop.cpp
  src/storage/graphar_catalog.cpp
  src/storage/graphar_schema_entry.cpp
  src/storage/graphar_storage.cpp
  src/storage/graphar_table_entry.cpp
  src/storage/graphar_transaction.cpp
  src/storage/graphar_transaction_manager.cpp
  src/storage/graphar_table_set.cpp
  src/utils/global_log_manager.cpp
)

# =============================================
# Dependency Management
# =============================================

# --- Arrow ---
find_package(Arrow REQUIRED)
if(NOT Arrow_FOUND)
  include(FetchContent)
  FetchContent_Declare(
    arrow
    GIT_REPOSITORY https://github.com/apache/arrow.git
    GIT_TAG apache-arrow-15.0.2
  )
  set(ARROW_BUILD_STATIC ON)
  set(ARROW_PARQUET ON)
  set(ARROW_ORC ON)
  set(ARROW_WITH_ZLIB ON)
  set(ARROW_WITH_ZSTD ON)
  FetchContent_MakeAvailable(arrow)
endif()

# --- GraphAR ---
include(FetchContent)
FetchContent_Declare(
  graphar
  GIT_REPOSITORY https://github.com/apache/incubator-graphar.git
  GIT_TAG 1cca4035f271fd117406d1d555a5ce1d3cd7ad51
)
FetchContent_MakeAvailable(graphar)

# =============================================
# DuckDB Extension Loading
# =============================================
if(LOAD_TESTS)
  find_package(duckdb REQUIRED HINTS ${DUCKDB_ROOT_DIR})
endif()

duckdb_extension_load(duckdb_graphar
  SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}
  LOAD_TESTS ${LOAD_TESTS}
  SOURCES ${EXTENSION_SOURCES}  # EXPLICIT SOURCE FILES
)

target_include_directories(duckdb_graphar PRIVATE
  src/include
  ${graphar_SOURCE_DIR}/cpp/thirdparty
  ${Arrow_INCLUDE_DIRS}
)

target_link_libraries(duckdb_graphar PRIVATE
  Arrow::arrow_shared
  graphar
)

# =============================================
# Loadable Extension (for CI/CD/Install)
# =============================================
if(BUILD_LOADABLE_EXTENSION)
  add_library(duckdb_graphar_loadable SHARED ${EXTENSION_SOURCES})
  target_link_libraries(duckdb_graphar_loadable PRIVATE 
    Arrow::arrow_shared 
    graphar
  )
  target_include_directories(duckdb_graphar_loadable PRIVATE
    src/include
    ${graphar_SOURCE_DIR}/cpp/thirdparty
    ${Arrow_INCLUDE_DIRS}
  )
  set_target_properties(duckdb_graphar_loadable PROPERTIES
    OUTPUT_NAME "duckdb_graphar_extension"
    PREFIX ""
  )
  
  install(TARGETS duckdb_graphar_loadable
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  )
endif()

# =============================================
#  Installation Support
# =============================================
install(TARGETS graphar
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

# =============================================
# Testing Support
# =============================================
if(LOAD_TESTS)
  enable_testing()
  add_subdirectory(test)
endif()
