
# hunter_config(Boost VERSION 1.76.0)
hunter_config(bcos-framework VERSION 3.0.0-5e937164
    URL https://github.com/FISCO-BCOS/bcos-framework/archive/e88edc774df5f2d44e0f042992328581197a9085.tar.gz
    SHA1 65be827a5e8d82e5adb05a7e5c3b8701166dd552
    CMAKE_ARGS HUNTER_PACKAGE_LOG_BUILD=ON HUNTER_PACKAGE_LOG_INSTALL=ON
)

hunter_config(rocksdb VERSION 6.19.3
    URL https://github.com/facebook/rocksdb/archive/refs/tags/v6.19.3.tar.gz
    SHA1 27c18025d739e83fe3819e48f2f4dfcc43526462
)
