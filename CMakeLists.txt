cmake_minimum_required(VERSION 3.12)
set(CMAKE_OSX_DEPLOYMENT_TARGET "10.15" CACHE STRING "Minimum OS X deployment version")

# import bcos-cmake-scripts & hunter & bcos-framework
include(FetchContent)
set(BCOS_CMAKE_SCRIPTS_DIR "${CMAKE_SOURCE_DIR}/cmake/scripts")
FetchContent_Declare(bcos-cmake-scripts
    PREFIX ${CMAKE_SOURCE_DIR}/cmake/deps
    DOWNLOAD_NO_PROGRESS 1
    GIT_REPOSITORY https://github.com/FISCO-BCOS/bcos-cmake-scripts.git
    GIT_TAG 8727db50f2b505a178bdf138acb99e78ab680531
    SOURCE_DIR ${BCOS_CMAKE_SCRIPTS_DIR}
    UPDATE_COMMAND git fetch
    LOG_CONFIGURE 1
    LOG_DOWNLOAD 1
    LOG_UPDATE 1
    LOG_BUILD 1
    LOG_INSTALL 1
)

FetchContent_MakeAvailable(bcos-cmake-scripts)
list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake/ ${BCOS_CMAKE_SCRIPTS_DIR})

# init hunter
set(HUNTER_PACKAGE_LOG_BUILD ON CACHE STRING "Hunter output build info to log")
set(HUNTER_PACKAGE_LOG_INSTALL ON CACHE STRING "Hunter output install info to log")
set(HUNTER_STATUS_DEBUG OFF CACHE STRING "Hunter debug output")
set(HUNTER_USE_CACHE_SERVERS NO CACHE STRING "Download cache from cache server")
set(HUNTER_CONFIGURATION_TYPES Release CACHE STRING "Build type of Hunter packages")
# set(HUNTER_STATUS_PRINT OFF CACHE STRING "Hunter status output")

# set(HUNTER_CONFIGURATION_TYPES Release CACHE STRING "Build type of Hunter packages")
include(HunterGate)
HunterGate(
    URL "https://github.com/FISCO-BCOS/hunter/archive/8c81ea454c0c268b257da1fe3e3f2f69bd59a67d.tar.gz"
    SHA1 "13292adf48c4c053ff2ceb7173631a296c08d1ac"
    FILEPATH "${CMAKE_SOURCE_DIR}/cmake/config.cmake"
)
# set CMP0074 to NEW to suppress warnning, https://cmake.org/cmake/help/latest/policy/CMP0074.html
cmake_policy(VERSION 3.12)
cmake_policy(SET CMP0074 NEW)

project(bcos-storage)
set(include_dir ${CMAKE_SOURCE_DIR}/include)

# basic setting
include(Options)
configure_project()
include(CompilerSettings)

# install bcos-framework
include(InstallBcosFrameworkDependencies)

# import tars project
add_subdirectory(src)

# FIXME: add Install instructions
# install(DIRECTORY ${include_dir}/
#     DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})