cmake_minimum_required(VERSION 3.14)
set(CMAKE_OSX_DEPLOYMENT_TARGET "11.3" CACHE STRING "Minimum OS X deployment version")

if (NOT DEFINED URL_BASE)
    set(URL_BASE "github.com")
endif()

# import bcos-cmake-scripts & hunter & bcos-framework
include(FetchContent)
set(BCOS_CMAKE_SCRIPTS_DIR "${CMAKE_SOURCE_DIR}/cmake/scripts")
FetchContent_Declare(bcos-cmake-scripts
    PREFIX ${CMAKE_SOURCE_DIR}/cmake/deps
    DOWNLOAD_NO_PROGRESS 1
    GIT_REPOSITORY https://${URL_BASE}/FISCO-BCOS/bcos-cmake-scripts.git
    GIT_TAG dev
    SOURCE_DIR ${BCOS_CMAKE_SCRIPTS_DIR}
    LOG_CONFIGURE 1
    LOG_DOWNLOAD 1
    LOG_UPDATE 1
    LOG_BUILD 1
    LOG_INSTALL 1
)

FetchContent_MakeAvailable(bcos-cmake-scripts)
list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake/ ${BCOS_CMAKE_SCRIPTS_DIR})

# init hunter
set(HUNTER_PACKAGE_LOG_BUILD ON CACHE STRING "Hunter output build info to log" FORCE)
set(HUNTER_PACKAGE_LOG_INSTALL ON CACHE STRING "Hunter output install info to log" FORCE)
set(HUNTER_STATUS_DEBUG OFF CACHE STRING "Hunter debug output" FORCE)
set(HUNTER_USE_CACHE_SERVERS NO CACHE STRING "Download cache from cache server" FORCE)
set(HUNTER_CONFIGURATION_TYPES Release CACHE STRING "Build type of Hunter packages")
# set(HUNTER_STATUS_PRINT OFF CACHE STRING "Hunter status output")

include(HunterGate)
HunterGate(
    URL "https://${URL_BASE}/FISCO-BCOS/hunter/archive/b55a347258b0fc3eda47f4ccd3e2c0db595b4957.tar.gz"
    SHA1 "9331d1e70cee5498bd59ada13aa68278cbb9a2d6"
    FILEPATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/config.cmake"
)

# set CMP0074 to NEW to suppress warnning, https://cmake.org/cmake/help/latest/policy/CMP0074.html
cmake_policy(VERSION 3.12)
cmake_policy(SET CMP0074 NEW)
cmake_policy(SET CMP0079 NEW)
cmake_policy(SET CMP0063 NEW)

project(bcos-storage VERSION "3.0.0")

# basic setting
include(Options)
configure_project()
include(CompilerSettings)
include(InstallConfig)

hunter_add_package(zstd)
find_package(zstd CONFIG REQUIRED)

if(USE_TiKV)
    message(STATUS "USE_TiKV: ${USE_TiKV}")
    include(ProjectTiKVClient)
endif()

# install bcos-framework
include(InstallBcosFrameworkDependencies)

set(STORAGE_TARGET "bcos-storage")
add_subdirectory(bcos-storage)

option(USE_TiKV "build tikv storage or not" OFF)

install(
    DIRECTORY "bcos-storage"
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/bcos-storage"
    FILES_MATCHING PATTERN "*.h"
)

option(TOOLS "build tools" OFF)
if(TOOLS)
    add_subdirectory(tools)
endif()

if (TESTS)
    set(USE_TiKV ON)
    enable_testing()
    set(ENV{CTEST_OUTPUT_ON_FAILURE} True)
    add_subdirectory(test)
endif()
