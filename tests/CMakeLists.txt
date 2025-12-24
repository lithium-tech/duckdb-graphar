log_stage("Declaring Catch2")
include(FetchContent)
FetchContent_Declare(
    Catch2
    GIT_REPOSITORY https://github.com/catchorg/Catch2.git
    GIT_TAG        v3.5.2 # The stable version of catch2
)
FetchContent_MakeAvailable(Catch2)
get_property(catch_targets GLOBAL PROPERTY Catch2_IMPORTED_TARGETS)
log_done()

get_target_property(CATCH2_INCLUDE_DIRS Catch2::Catch2 INTERFACE_INCLUDE_DIRECTORIES)

log_stage("Building unittest_graphar")

add_subdirectory(table_functions)
add_subdirectory(scalar_functions)

add_executable(unittest_graphar ${ALL_OBJECT_FILES})

target_link_libraries(unittest_graphar
    PRIVATE
        duckdb_graphar_extension       
        Catch2::Catch2WithMain         
)

target_include_directories(unittest_graphar PRIVATE
    ${DUCKDB_INCLUDES}
    ${EXTENSION_INCLUDES}
)

# Integrate with ctest for easy test execution.
add_test(NAME ${DUCKDB_EXTENSION_NAME}_unittest COMMAND unittest_graphar)
log_done()