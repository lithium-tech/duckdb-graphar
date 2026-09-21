set(USAGE_ANALYTICS_DIR ${CMAKE_CURRENT_LIST_DIR})
set(USAGE_ANALYTICS_INCLUDE_DIR ${USAGE_ANALYTICS_DIR}/include)

find_package(Boost REQUIRED COMPONENTS json thread random)

file(GLOB_RECURSE USAGE_ANALYTICS_SOURCES CONFIGURE_DEPENDS
    ${USAGE_ANALYTICS_DIR}/src/*.cpp
)

add_library(product_usage_analytics_lib OBJECT ${USAGE_ANALYTICS_SOURCES})

target_include_directories(product_usage_analytics_lib PUBLIC
    ${USAGE_ANALYTICS_INCLUDE_DIR}
    ${Boost_INCLUDE_DIRS}
)

target_include_directories(product_usage_analytics_lib PRIVATE
    ${PROJECT_INC_DIR}
)

target_link_libraries(product_usage_analytics_lib PUBLIC
    ${Boost_LIBRARIES}
)
