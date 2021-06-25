
# hunter_config(Boost VERSION 1.76.0)
hunter_config(bcos-framework VERSION 3.0.0-local
    URL "https://${URL_BASE}/FISCO-BCOS/bcos-framework/archive/98a2530574ebf546fe38f92bddf3fe33305d9057.tar.gz"
    SHA1 be626ecd549151564dcec75cd4f2b2579188c237
    CMAKE_ARGS HUNTER_PACKAGE_LOG_BUILD=ON HUNTER_PACKAGE_LOG_INSTALL=ON
)

hunter_config(rocksdb VERSION 6.19.3
	URL https://${URL_BASE}/facebook/rocksdb/archive/refs/tags/v6.19.3.tar.gz
    SHA1 27c18025d739e83fe3819e48f2f4dfcc43526462
    CMAKE_ARGS WITH_TESTS=OFF
    WITH_GFLAGS=OFF
    WITH_BENCHMARK_TOOLS=OFF
    WITH_CORE_TOOLS=OFF
    WITH_TOOLS=OFF
    PORTABLE=ON
    FAIL_ON_WARNINGS=OFF
    WITH_ZSTD=ON
)
