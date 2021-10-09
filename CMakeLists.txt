cmake_minimum_required(VERSION 3.14)

include(cmake/HunterGate.cmake)
HunterGate(
    URL "https://github.com/FISCO-BCOS/hunter/archive/9ea365415828102627dcc933295e25e8feff7d93.tar.gz"
    SHA1 "b40d19ddd9939f557b70c69e45f8744ff17583ba"
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

if (NOT Protobuf_INCLUDE_DIR)
    include (cmake/find_protobuf.cmake)
endif ()

if (NOT gRPC_FOUND)
    include (cmake/find_grpc.cmake)
endif ()

if (NOT Poco_Foundation_LIBRARY)
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
