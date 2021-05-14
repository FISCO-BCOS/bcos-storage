
include(GNUInstallDirs)
file(GLOB SRC_LIST "*.cpp" "./RocksDBAdapter/*.cpp")
file(GLOB HEADERS "*.h" "./RocksDBAdapter/*.h")

hunter_add_package(zstd)
find_package(zstd CONFIG REQUIRED)
# list(APPEND CMAKE_MODULE_PATH "${zstd_DIR}/cmake")
set(zstd_ROOT ${ZSTD_ROOT})
hunter_add_package(rocksdb)
find_package(RocksDB CONFIG REQUIRED)

add_library(${STORAGE_TARGET} ${SRC_LIST} ${HEADERS})

find_package(Boost CONFIG QUIET REQUIRED serialization)
find_package(Threads REQUIRED)

target_link_libraries(${STORAGE_TARGET} PUBLIC tbb::tbb zstd::libzstd_static Threads::Threads Boost::serialization RocksDB::rocksdb bcos-framework::utilities)
# target_compile_options(storage PRIVATE -Wno-error)

install(
    DIRECTORY "./"
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/bcos-storage"
    FILES_MATCHING PATTERN "*.h"
)

install(
    TARGETS ${STORAGE_TARGET}
    EXPORT "${TARGETS_EXPORT_NAME}"
    LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
    INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/bcos-storage"
)