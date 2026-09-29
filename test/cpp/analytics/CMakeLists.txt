file(GLOB_RECURSE PUA_PRODUCTION_SINK_SOURCES CONFIGURE_DEPENDS
    "${EXTENSION_ROOT_DIR}/src/sinks/*.cpp"
)
file(GLOB_RECURSE PUA_TEST_SINK_SOURCES CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_LIST_DIR}/sinks/*.cpp"
)

add_executable(unittest_product_usage_analytics
    test_product_usage_analytics.cpp
    ${PUA_PRODUCTION_SINK_SOURCES}
    ${PUA_TEST_SINK_SOURCES}
)

target_link_libraries(unittest_product_usage_analytics
    PRIVATE
        product_usage_analytics_lib
        Boost::log
        Boost::filesystem
        libuv::uv_a
        uvw::uvw
        Catch2::Catch2WithMain
)

target_include_directories(unittest_product_usage_analytics
    PRIVATE
        ${EXTENSION_ROOT_DIR}/include
)

add_test(
    NAME product_usage_analytics_unittest
    COMMAND unittest_product_usage_analytics
    WORKING_DIRECTORY "${EXTENSION_ROOT_DIR}"
)
