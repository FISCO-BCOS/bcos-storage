
include(GNUInstallDirs)
# compile rocksDB

# set(zstd_ROOT ${ZSTD_ROOT})
hunter_add_package(rocksdb)
find_package(RocksDB CONFIG REQUIRED)

add_library(${STORAGE_TARGET} Common.cpp RocksDBStorage.cpp)

find_package(Boost CONFIG QUIET REQUIRED serialization)
find_package(Threads REQUIRED)

# target_link_libraries(${STORAGE_TARGET} PUBLIC bcos::tikv_client tbb::tbb Boost::serialization RocksDB::rocksdb zstd::libzstd_static Threads::Threads bcos-framework::utilities)
target_link_libraries(${STORAGE_TARGET} PUBLIC tbb::tbb Boost::serialization RocksDB::rocksdb zstd::libzstd_static Threads::Threads bcos-framework::utilities)

install(
    TARGETS ${STORAGE_TARGET}
    EXPORT "${TARGETS_EXPORT_NAME}"
    LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
    INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/bcos-storage"
)
