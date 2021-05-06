
# hunter_config(Boost VERSION 1.76.0)
hunter_config(bcos-framework VERSION 3.0.0-5e937164
    URL https://github.com/FISCO-BCOS/bcos-framework/archive/5e937164b78a80a8d81397bbaf9f28d6488b874d.tar.gz
    SHA1 0526295ca6313c87bba0c96d85284540c37e08bb
    CMAKE_ARGS HUNTER_PACKAGE_LOG_BUILD=ON HUNTER_PACKAGE_LOG_INSTALL=ON
)

hunter_config(rocksdb VERSION 6.19.3
    URL https://github.com/facebook/rocksdb/archive/refs/tags/v6.19.3.tar.gz
    SHA1 27c18025d739e83fe3819e48f2f4dfcc43526462
)
