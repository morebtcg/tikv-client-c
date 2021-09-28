if (APPLE)
    set(OPENSSL_ROOT_DIR /usr/local/opt/openssl)
    # if(NOT TESTS)
    #     set(OPENSSL_USE_STATIC_LIBS TRUE)
    # endif()
endif()
find_package(gRPC REQUIRED)
message(STATUS "Using gRPC: ${gRPC_VERSION} : ${gRPC_INCLUDE_DIRS}, ${gRPC_LIBRARIES}, ${gRPC_CPP_PLUGIN}")

