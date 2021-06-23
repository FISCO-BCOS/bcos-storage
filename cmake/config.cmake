
# hunter_config(Boost VERSION 1.76.0)
hunter_config(bcos-framework VERSION 3.0.0-local
    URL "https://${URL_BASE}/FISCO-BCOS/bcos-framework/archive/c178a7ed6e92ce3ea58e5df6d632c3d1cd4877dd.tar.gz"
    SHA1 12f8367acc31b2b05d411b744677468c03c2aeca
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
