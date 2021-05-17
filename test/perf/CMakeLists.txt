# file(GLOB SRC_LIST "*.cpp")
# file(GLOB HEADERS "*.h")

find_package(Boost CONFIG QUIET REQUIRED program_options)
find_package(wedpr-crypto CONFIG QUIET REQUIRED)

add_executable(multiget multiget.cpp)
target_compile_options(multiget PRIVATE -Wno-error)
target_include_directories(multiget PRIVATE . ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(multiget PUBLIC ${STORAGE_TARGET} Boost::program_options)

add_executable(table_benchmark table_benchmark.cpp)
target_compile_options(table_benchmark PRIVATE -Wno-error)
target_include_directories(table_benchmark PRIVATE . ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(table_benchmark PUBLIC ${STORAGE_TARGET} bcos-framework::utilities bcos-framework::table
            wedpr-crypto::crypto Boost::program_options)
