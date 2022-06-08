cmake_minimum_required(VERSION 3.14)

# set(HUNTER_CONFIGURATION_TYPES Debug CACHE STRING "Build type of Hunter packages")
if (NOT DEFINED URL_BASE)
    set(URL_BASE "github.com")
endif ()
include(cmake/HunterGate.cmake)
HunterGate(
    URL "https://${URL_BASE}/FISCO-BCOS/hunter/archive/162ff7cde3d843e43afdab8fe3214275c35ef168.tar.gz"
    SHA1 "0ba8099fe89beb0d44986c750fe0d44040725d99"
    FILEPATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake/config.cmake
)

project(kvClient)
set (CMAKE_CXX_STANDARD 17)
set (CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -Wno-narrowing")
set (CMAKE_EXPORT_COMPILE_COMMANDS ON)
set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/")
enable_testing()

hunter_add_package(Protobuf)
# hunter_add_package(OpenSSL)
hunter_add_package(PocoCpp)
hunter_add_package(gRPC)
# hunter_add_package(c-ares)
hunter_add_package(Boost COMPONENTS all)
find_package(Boost CONFIG QUIET REQUIRED thread context)
find_package(Protobuf CONFIG REQUIRED)

if (NOT Protobuf_LIBRARY)
    include (cmake/find_protobuf.cmake)
endif ()

if (NOT gRPC_FOUND)
    include (cmake/find_grpc.cmake)
endif ()

if (NOT Poco_LIBRARIES)
    include (cmake/find_poco.cmake)
endif()

if (NOT KVPROTO_FOUND)
    include (cmake/find_kvproto.cmake)
endif ()

if (NOT GTEST_FOUND AND ENABLE_TESTS)
    include (cmake/find_gtest.cmake)
endif()

add_subdirectory (third_party)
add_subdirectory (src)
