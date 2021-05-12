
# hunter_config(Boost VERSION 1.76.0)
hunter_config(bcos-framework VERSION 3.0.0-5e937164
    URL https://github.com/FISCO-BCOS/bcos-framework/archive/f4a6d14b4c304933535b5f7d1491d141cac47aa4.tar.gz
    SHA1 a11ec8489f84fd67d702cd0148e092a629f867bd
    CMAKE_ARGS HUNTER_PACKAGE_LOG_BUILD=ON HUNTER_PACKAGE_LOG_INSTALL=ON
)

hunter_config(rocksdb VERSION 6.19.3
    URL https://github.com/facebook/rocksdb/archive/refs/tags/v6.19.3.tar.gz
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
