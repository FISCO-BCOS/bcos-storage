
# hunter_config(Boost VERSION 1.76.0-b1)
hunter_config(bcos-framework VERSION 3.0.0-1442458d
    URL https://github.com/FISCO-BCOS/bcos-framework/archive/1442458dd65d3cadf1a047a9ce0c9457cb69084e.tar.gz
    SHA1 e0968092d5c7d78c7a4bf1b0e46788770ba6e117
    CMAKE_ARGS HUNTER_PACKAGE_LOG_BUILD=ON HUNTER_PACKAGE_LOG_INSTALL=ON
)

hunter_config(rocksdb VERSION 6.19.3
    URL https://github.com/facebook/rocksdb/archive/refs/tags/v6.19.3.tar.gz
    SHA1 27c18025d739e83fe3819e48f2f4dfcc43526462
)
