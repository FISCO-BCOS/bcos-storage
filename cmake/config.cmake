
# hunter_config(Boost VERSION 1.76.0)
hunter_config(bcos-framework VERSION 3.0.0-5e937164
    URL https://github.com/FISCO-BCOS/bcos-framework/archive/bfe65663ecf0c8f17bcdb39428fdc120ff62c587.tar.gz
    SHA1 17ba9b441551f94e48cb2f533cc1973f4418413d
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
