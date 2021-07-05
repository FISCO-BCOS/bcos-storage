
# hunter_config(Boost VERSION 1.76.0)
hunter_config(bcos-framework VERSION 3.0.0-local
    URL "https://${URL_BASE}/FISCO-BCOS/bcos-framework/archive/8f63febe36bdaf9616ec07a4ffc24750d08dfd6a.tar.gz"
    SHA1 252d11f8ecce814512e576cb9b36a4126e2888f1
    CMAKE_ARGS HUNTER_PACKAGE_LOG_BUILD=ON HUNTER_PACKAGE_LOG_INSTALL=ON
)

hunter_config(rocksdb VERSION 6.20.3
	URL https://${URL_BASE}/facebook/rocksdb/archive/refs/tags/v6.20.3.tar.gz
    SHA1 64e4e6031820026c051d6e2072c0197e3bce1643
    CMAKE_ARGS WITH_TESTS=OFF
    WITH_GFLAGS=OFF
    WITH_BENCHMARK_TOOLS=OFF
    WITH_CORE_TOOLS=OFF
    WITH_TOOLS=OFF
    PORTABLE=ON
    FAIL_ON_WARNINGS=OFF
    WITH_ZSTD=ON
)
