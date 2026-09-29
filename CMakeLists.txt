cmake_minimum_required(VERSION 3.20)

set(TARGET_NAME duckdb_graphar)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)
set(EXTENSION_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR} CACHE STRING "Extension root directory")

project(${TARGET_NAME})

# Get git commit hash for this extension
find_package(Git)
if(Git_FOUND)
    # Get extension git commit hash
    execute_process(
        COMMAND ${GIT_EXECUTABLE} log -1 --format=%h
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        OUTPUT_VARIABLE EXTENSION_GIT_COMMIT_HASH
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
        RESULT_VARIABLE EXTENSION_GIT_RESULT
    )
    if(EXTENSION_GIT_RESULT)
        set(EXTENSION_GIT_COMMIT_HASH "unknown")
    endif()
else()
    set(EXTENSION_GIT_COMMIT_HASH "unknown")
endif()

# Get build timestamp
string(TIMESTAMP BUILD_TIMESTAMP "%Y-%m-%d %H:%M:%S%z")

# Add compile definitions for version information
add_definitions(-DEXTENSION_GIT_COMMIT_HASH="${EXTENSION_GIT_COMMIT_HASH}")

# Only graphar_info.cpp exposes build metadata, so scope the ever-changing
# timestamp to that file; a global define would recompile every source on
# each configure.
set_source_files_properties(
    ${EXTENSION_ROOT_DIR}/src/functions/table/graphar_info.cpp
    PROPERTIES COMPILE_DEFINITIONS EXTENSION_BUILD_TIMESTAMP="${BUILD_TIMESTAMP}")

# Print version information during configuration
message(STATUS "Extension Git Commit: ${EXTENSION_GIT_COMMIT_HASH}")
message(STATUS "Build Timestamp: ${BUILD_TIMESTAMP}")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(UNIX AND NOT APPLE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -static-libstdc++ -static-libgcc")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libstdc++ -static-libgcc")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -static-libstdc++ -static-libgcc")
endif()

if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/third_party/extension_deps.cmake")
    include("${CMAKE_CURRENT_SOURCE_DIR}/third_party/extension_deps.cmake")
endif()

message("My roots: ${Boost_ROOT} ${ARROW_ROOT} ${GRAPHAR_ROOT}")
if(ARROW_ROOT)
    list(APPEND CMAKE_PREFIX_PATH ${ARROW_ROOT})
endif()

if(GRAPHAR_ROOT)
    list(APPEND CMAKE_PREFIX_PATH ${GRAPHAR_ROOT})
endif()

if(LIBUV_ROOT)
    list(APPEND CMAKE_PREFIX_PATH ${LIBUV_ROOT})
endif()

if(UVW_ROOT)
    list(APPEND CMAKE_PREFIX_PATH ${UVW_ROOT})
endif()

find_package(Boost COMPONENTS json thread random log filesystem REQUIRED)
find_package(Arrow REQUIRED CONFIG)
find_package(ArrowDataset REQUIRED CONFIG)
find_package(ArrowAcero REQUIRED CONFIG)
find_package(Parquet REQUIRED CONFIG)

find_package(graphar REQUIRED CONFIG)
find_package(libuv REQUIRED CONFIG)
find_package(uvw REQUIRED CONFIG)

# GraphAr's target carries `-Wl,--exclude-libs,ALL`, which would hide the
# generated-extension-loader symbols in libduckdb.so that `unittest` needs to
# run the SQL tests; strip just that flag (Arrow/Parquet linkage is unaffected).
if(TARGET graphar)
    get_target_property(_gar_link_libs graphar INTERFACE_LINK_LIBRARIES)
    if(_gar_link_libs)
        list(REMOVE_ITEM _gar_link_libs "-Wl,--exclude-libs,ALL")
        set_target_properties(graphar PROPERTIES INTERFACE_LINK_LIBRARIES "${_gar_link_libs}")
    endif()
    unset(_gar_link_libs)
endif()

include_directories(include)

# Add mini-yaml include path from GraphAr source
if(GRAPHAR_SRC_DIR)
    include_directories(${GRAPHAR_SRC_DIR}/cpp/thirdparty)
elseif(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/third_party/graphar/src")
    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party/graphar/src/cpp/thirdparty)
endif()

set(PROJECT_INC_DIR ${EXTENSION_ROOT_DIR}/include)
add_subdirectory(analytics)

file(GLOB_RECURSE EXTENSION_SOURCES CONFIGURE_DEPENDS "${EXTENSION_ROOT_DIR}/src/*.cpp")
file(GLOB_RECURSE EXTENSION_SINK_SOURCES CONFIGURE_DEPENDS "${EXTENSION_ROOT_DIR}/src/sinks/*.cpp")
list(REMOVE_ITEM EXTENSION_SOURCES ${EXTENSION_SINK_SOURCES})

message(LOG "Building static extension")
build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
target_sources(${EXTENSION_NAME} PRIVATE $<TARGET_OBJECTS:product_usage_analytics_lib>)
# Sink translation units are compiled directly by the final consumer so their
# self-registration cannot be dropped when the static extension archive is linked.
target_sources(${EXTENSION_NAME} INTERFACE "$<BUILD_INTERFACE:${EXTENSION_SINK_SOURCES}>")
target_include_directories(${EXTENSION_NAME}
    PRIVATE
        ${EXTENSION_ROOT_DIR}/analytics/usage_analytics/include
    INTERFACE
        "$<BUILD_INTERFACE:${EXTENSION_ROOT_DIR}/include>"
        "$<BUILD_INTERFACE:${EXTENSION_ROOT_DIR}/analytics/usage_analytics/include>"
)

message(LOG "Building loadable extension")
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES} ${EXTENSION_SINK_SOURCES})
target_sources(${LOADABLE_EXTENSION_NAME} PRIVATE $<TARGET_OBJECTS:product_usage_analytics_lib>)
target_include_directories(${LOADABLE_EXTENSION_NAME}
    PRIVATE
        ${EXTENSION_ROOT_DIR}/analytics/usage_analytics/include
)

set(EXTENSION_LIBRARIES
        graphar
        ${Boost_LIBRARIES}
        libuv::uv_a
        uvw::uvw)

target_link_libraries(${EXTENSION_NAME} ${EXTENSION_LIBRARIES})
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${EXTENSION_LIBRARIES})

if(TARGET Arrow::arrow_static)
    get_target_property(_arrow_incdirs Arrow::arrow_static INTERFACE_INCLUDE_DIRECTORIES)
    if(_arrow_incdirs)
        target_include_directories(${EXTENSION_NAME} PRIVATE ${_arrow_incdirs})
        target_include_directories(${LOADABLE_EXTENSION_NAME} PRIVATE ${_arrow_incdirs})
    endif()
endif()

install(TARGETS ${EXTENSION_NAME}
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

# C++ unit tests (test/cpp/, own Catch2 binary `unittest_graphar`).
option(BUILD_EXTENSION_UNIT_TESTS "Build the extension's C++ unit-test binary (test/cpp/)" OFF)
if(BUILD_EXTENSION_UNIT_TESTS)
    # Arrow / GraphAr install roots come from the Makefile-provided ARROW_ROOT /
    # GRAPHAR_ROOT (also consumed via find_package above).
    if(NOT DEFINED ARROW_ROOT)
        set(ARROW_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/third_party/arrow/install")
    endif()
    if(NOT DEFINED GRAPHAR_ROOT)
        set(GRAPHAR_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/third_party/graphar/install")
    endif()
    set(ARROW_INSTALL_DIR "${ARROW_ROOT}")
    set(GRAPHAR_INSTALL_DIR "${GRAPHAR_ROOT}")
    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/third_party/graphar/src/cpp/thirdparty")
        set(GRAPHAR_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party/graphar/src")
    endif()

    add_subdirectory(test/cpp test/cpp)
endif()
