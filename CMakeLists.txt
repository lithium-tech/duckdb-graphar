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
add_definitions(-DEXTENSION_BUILD_TIMESTAMP="${BUILD_TIMESTAMP}")

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
    # Temp fix dublicate arrow on linux
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--allow-multiple-definition")
endif()

if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/third_party/extension_deps.cmake")
    include("${CMAKE_CURRENT_SOURCE_DIR}/third_party/extension_deps.cmake")
endif()

message("My roots: ${ARROW_ROOT} ${GRAPHAR_ROOT}")
if(ARROW_ROOT)
    list(APPEND CMAKE_PREFIX_PATH ${ARROW_ROOT})
endif()

if(GRAPHAR_ROOT)
    list(APPEND CMAKE_PREFIX_PATH ${GRAPHAR_ROOT})
endif()

find_package(Arrow REQUIRED CONFIG)
find_package(ArrowDataset REQUIRED CONFIG)
find_package(ArrowAcero REQUIRED CONFIG)
find_package(Parquet REQUIRED CONFIG)

find_package(graphar REQUIRED CONFIG)

include_directories(include)

file(GLOB_RECURSE EXTENSION_SOURCES CONFIGURE_DEPENDS "${EXTENSION_ROOT_DIR}/src/*.cpp")

message(LOG "Building static extension")
build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})

message(LOG "Building loadable extension")
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

set(EXTENSION_LIBRARIES
        graphar
        Arrow::arrow_static)

target_link_libraries(${EXTENSION_NAME} ${EXTENSION_LIBRARIES})
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${EXTENSION_LIBRARIES})

install(TARGETS ${EXTENSION_NAME}
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")