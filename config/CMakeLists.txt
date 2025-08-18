# =============================================
# Build Targets
# =============================================

file(GLOB_RECURSE EXTENSION_SOURCES CONFIGURE_DEPENDS "${EXTENSION_ROOT_DIR}/src/*.cpp")

log_stage("Building static extension")
build_static_extension(graphar_duck ${EXTENSION_SOURCES})
log_done()

if (BUILD_LOADABLE_EXTENSION)
    log_stage("Building loadable extension")
    build_loadable_extension(graphar_duck ${EXTENSION_SOURCES})
    log_done()
endif()

set(graphar_SOURCE_DIR "${CMAKE_BINARY_DIR}/_deps/graphar-src")
set(arrow_SOURCE_DIR "${CMAKE_BINARY_DIR}/_deps/arrow-src")

set(EXTENSION_INCLUDES
     $<BUILD_INTERFACE:${EXTENSION_ROOT_DIR}/include>
    $<BUILD_INTERFACE:${graphar_SOURCE_DIR}/cpp/thirdparty>  # GraphAr thirdparty
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}/graphar-install/include>  # GraphAr main includes (?)
    # $<BUILD_INTERFACE:${graphar_BINARY_DIR}/cpp/src>
     $<BUILD_INTERFACE:${duckdb_SOURCE_DIR}/src/include>  # DuckDB internal (?)
    $<BUILD_INTERFACE:${arrow_SOURCE_DIR}/cpp/src>  # Arrow main includes (?)
    $<BUILD_INTERFACE:${arrow_BINARY_DIR}/cpp/src>  # Generated Arrow headers (?)
    # $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
)
target_include_directories(graphar_duck_extension PUBLIC ${EXTENSION_INCLUDES} $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
if (BUILD_LOADABLE_EXTENSION)
    target_include_directories(graphar_duck_loadable_extension PUBLIC ${EXTENSION_INCLUDES} $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
endif()

target_link_libraries(graphar_duck_extension
    duckdb_static
    arrow::arrow_shared
    graphar::graphar_shared
)

if (BUILD_LOADABLE_EXTENSION)
    target_link_libraries(graphar_duck_loadable_extension
        arrow::arrow_shared
        graphar::graphar_shared
    )
endif()

# =============================================
# Installation
# =============================================
log_stage("Installing...")
set(INSTALL_LIB_DIR "${CMAKE_INSTALL_LIBDIR}" CACHE PATH "Library installation directory")

install(
  TARGETS graphar_duck_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

if(BUILD_LOADABLE_EXTENSION)
    install(TARGETS graphar_duck_loadable_extension
        LIBRARY DESTINATION ${INSTALL_LIB_DIR}
    )
endif()

install(DIRECTORY include/ DESTINATION include
    FILES_MATCHING PATTERN "*.h" PATTERN "*.hpp"
)

log_done()
