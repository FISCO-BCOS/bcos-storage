
file(GLOB SRC_LIST "*.cpp" "./RocksDBAdapter/*.cpp")
file(GLOB HEADERS "*.h" "./RocksDBAdapter/*.h")

hunter_add_package(rocksdb)
find_package(rocksdb CONFIG REQUIRED)

add_library(storage ${SRC_LIST} ${HEADERS})

# target_link_libraries(storage PRIVATE blockverifier ethcore)
target_link_libraries(storage PUBLIC tbb::tbb rocksdb)
target_compile_options(storage PRIVATE -Wno-error)
target_include_directories(storage PUBLIC ${include_dir})
