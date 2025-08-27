# =============================================
# Build Targets
# =============================================

file(GLOB_RECURSE EXTENSION_SOURCES CONFIGURE_DEPENDS "${EXTENSION_ROOT_DIR}/src/*.cpp")

log_stage("Building static extension")
build_static_extension(duckdb_graphar ${EXTENSION_SOURCES})
log_done()

log_stage("Building loadable extension")
build_loadable_extension(duckdb_graphar ${EXTENSION_SOURCES})
log_done()

set(DEPS_INSTALL_DIR ${CMAKE_BINARY_DIR}/_deps)
set(ARROW_INSTALL_DIR ${DEPS_INSTALL_DIR}/arrow-install)
set(GRAPHAR_INSTALL_DIR ${DEPS_INSTALL_DIR}/graphar-install)

set(GRAPHAR_SOURCE_DIR ${DEPS_INSTALL_DIR}/graphar-src)

set(DUCKDB_EXTENSION_DIR ${duckdb_SOURCE_DIR}/extension)
set(DUCKDB_THIRD_PARTY_DIR ${duckdb_SOURCE_DIR}/third_party)

set(EXTENSION_INCLUDES
    $<BUILD_INTERFACE:${EXTENSION_ROOT_DIR}/include>
    $<BUILD_INTERFACE:${GRAPHAR_INSTALL_DIR}/include>
    $<BUILD_INTERFACE:${GRAPHAR_SOURCE_DIR}/cpp/thirdparty>
    $<BUILD_INTERFACE:${duckdb_SOURCE_DIR}/src/include>
    $<BUILD_INTERFACE:${arrow_SOURCE_DIR}/cpp/src>
    $<BUILD_INTERFACE:${DUCKDB_EXTENSION_DIR}>
    $<BUILD_INTERFACE:${DUCKDB_EXTENSION_DIR}/parquet/include>
    $<BUILD_INTERFACE:${DUCKDB_THIRD_PARTY_DIR}/parquet>
    $<BUILD_INTERFACE:${DUCKDB_THIRD_PARTY_DIR}/thrift>
    $<BUILD_INTERFACE:${DUCKDB_THIRD_PARTY_DIR}/fastpforlib>
    $<BUILD_INTERFACE:${DUCKDB_THIRD_PARTY_DIR}/lz4>
    $<BUILD_INTERFACE:${DUCKDB_THIRD_PARTY_DIR}/snappy>
    $<BUILD_INTERFACE:${DUCKDB_THIRD_PARTY_DIR}/miniz>
    $<BUILD_INTERFACE:${DUCKDB_THIRD_PARTY_DIR}/brotli/include>
    $<BUILD_INTERFACE:${DUCKDB_THIRD_PARTY_DIR}/zstd/include>
)

target_include_directories(duckdb_graphar_extension PUBLIC ${EXTENSION_INCLUDES} $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_include_directories(duckdb_graphar_loadable_extension PUBLIC ${EXTENSION_INCLUDES} $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

set(EXT_LIBS
    arrow::arrow_shared
    graphar::graphar_shared)

if (NOT APPLE)
    list(APPEND EXT_LIBS "-Wl,--disable-new-dtags") # Use rpath for ubuntu build
endif()


target_link_libraries(duckdb_graphar_extension
    duckdb_static
    ${EXT_LIBS}
)

target_link_libraries(duckdb_graphar_loadable_extension
    ${EXT_LIBS}
)

# =============================================
# Installation
# =============================================
log_stage("Installing...")
set(INSTALL_LIB_DIR "${CMAKE_INSTALL_LIBDIR}" CACHE PATH "Library installation directory")

install(
  TARGETS duckdb_graphar_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

install(TARGETS duckdb_graphar_loadable_extension
    LIBRARY DESTINATION ${INSTALL_LIB_DIR}
)

install(DIRECTORY include/ DESTINATION include
    FILES_MATCHING PATTERN "*.h" PATTERN "*.hpp"
)

log_done()
