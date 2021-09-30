# TODO We should make a bundled gPRC and protoobuf repository, instance of rely on system library.

# gRPC and relateds
if (APPLE)
    set(OPENSSL_ROOT_DIR /usr/local/opt/openssl)
    # if(NOT TESTS)
    #     set(OPENSSL_USE_STATIC_LIBS TRUE)
    # endif()
endif()
find_package (OpenSSL REQUIRED)
message (STATUS "Using ssl=${OPENSSL_FOUND}: ${OPENSSL_INCLUDE_DIR} : ${OPENSSL_LIBRARIES}")

find_package(Protobuf REQUIRED)
message(STATUS "Using protobuf: ${Protobuf_VERSION} : ${Protobuf_INCLUDE_DIRS}, ${Protobuf_LIBRARIES}")

include_directories(${PROTOBUF_INCLUDE_DIRS})

find_package(c-ares REQUIRED)
message(STATUS "Lib c-ares found")

find_package(ZLIB REQUIRED)
message(STATUS "Using ZLIB: ${ZLIB_INCLUDE_DIRS}, ${ZLIB_LIBRARIES}")

find_package(gRPC CONFIG REQUIRED)
message(STATUS "Using gRPC: ${gRPC_VERSION} : ${gRPC_DIR}")

message(STATUS "After init git submodule, Execute the below command to generate cpp files \nexport PATH=${GRPC_ROOT}:\$PATH && cd third_party/kvproto && bash scripts/generate_cpp.sh")
