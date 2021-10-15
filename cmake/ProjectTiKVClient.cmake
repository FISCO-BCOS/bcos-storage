include(FetchContent)

if (APPLE)
    set(SED_CMMAND sed -i .bkp)
else()
    set(SED_CMMAND sed -i)
endif()

hunter_add_package(OpenSSL)
find_package(OpenSSL REQUIRED)
hunter_add_package(Protobuf)
find_package(Protobuf CONFIG REQUIRED)
hunter_add_package(gRPC)
find_package(gRPC CONFIG REQUIRED)
hunter_add_package(PocoCpp)
find_package (Poco REQUIRED Foundation Net JSON Util)

set(ENV{PATH} ${GRPC_ROOT}/bin:$ENV{PATH})
FetchContent_Declare(tikv_client_project
  GIT_REPOSITORY https://${URL_BASE}/bxq2011hust/client-c.git
  GIT_TAG        989437a63b5b7b09584ba12720b3f20d2b4a941a
  # SOURCE_DIR     ${CMAKE_SOURCE_DIR}/deps/src/
  PATCH_COMMAND  export PATH=${GRPC_ROOT}/bin:\$PATH COMMAND protoc --version COMMAND bash third_party/kvproto/scripts/generate_cpp.sh COMMAND ${SED_CMMAND} "s#PUBLIC#PRIVATE#g" third_party/kvproto/cpp/CMakeLists.txt
  # LOG_BUILD true
)

if(NOT tikv_client_project_POPULATED)
  FetchContent_Populate(tikv_client_project)
  list(APPEND CMAKE_MODULE_PATH ${tikv_client_project_SOURCE_DIR}/cmake/)
  set(BUILD_SHARED_LIBS OFF)
  add_subdirectory(${tikv_client_project_SOURCE_DIR} ${tikv_client_project_BINARY_DIR})
  target_include_directories(kvproto PUBLIC ${GRPC_ROOT}/include)
  target_compile_options(fiu PRIVATE -Wno-error -Wno-unused-parameter)
  target_compile_options(kvproto PRIVATE -Wno-error -Wno-unused-parameter)
  target_compile_options(kv_client PRIVATE -Wno-error -Wno-unused-parameter)
endif()

# add_library(bcos::tikv_client INTERFACE IMPORTED)
# set_property(TARGET bcos::tikv_client PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${tikv_client_project_SOURCE_DIR}/include ${GRPC_ROOT}/include)
# set_property(TARGET bcos::tikv_client PROPERTY INTERFACE_LINK_LIBRARIES kv_client gRPC::grpc++ Poco::Foundation)

install(
  TARGETS kv_client fiu kvproto
  EXPORT "${TARGETS_EXPORT_NAME}"
  # LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  # RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
  # INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/bcos-storage"
)
