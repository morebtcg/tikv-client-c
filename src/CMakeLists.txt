set(kvClient_sources)

list(APPEND kvClient_sources kv/2pc.cc)
list(APPEND kvClient_sources kv/BCOS2pc.cc)
list(APPEND kvClient_sources kv/Backoff.cc)
list(APPEND kvClient_sources kv/Cluster.cc)
list(APPEND kvClient_sources kv/LockResolver.cc)
list(APPEND kvClient_sources kv/RegionCache.cc)
list(APPEND kvClient_sources kv/RegionClient.cc)
list(APPEND kvClient_sources kv/Rpc.cc)
list(APPEND kvClient_sources kv/Snapshot.cc)
list(APPEND kvClient_sources kv/Scanner.cc)
list(APPEND kvClient_sources pd/Client.cc)
list(APPEND kvClient_sources coprocessor/Client.cc)
list(APPEND kvClient_sources RedactHelpers.cc)

set(kvClient_INCLUDE_DIR ${kvClient_SOURCE_DIR}/include)
set(KVPROTO_INCLUDE_DIR ${kvClient_SOURCE_DIR}/third_party/kvproto/cpp/)

# Only enable fiu under test mode.
if (ENABLE_TESTS)
    add_definitions(-DFIU_ENABLE)
endif()

add_library(kv_client ${kvClient_sources})
target_include_directories(kv_client PUBLIC
    $<BUILD_INTERFACE:${KVPROTO_INCLUDE_DIR}>
    $<BUILD_INTERFACE:${kvClient_INCLUDE_DIR}>
    $<BUILD_INTERFACE:${fiu_include_dirs}>
    $<INSTALL_INTERFACE:include/pingcap>
    )
target_link_libraries(kv_client PUBLIC kvproto Poco::Foundation Poco::XML fiu Boost::thread Boost::context gRPC::grpc gRPC::grpc++ ${Protobuf_LIBRARY} ${OPENSSL_LIBRARIES})

include(GNUInstallDirs)
install(TARGETS kv_client fiu kvproto EXPORT tikvclientcTargets ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}")
install(DIRECTORY ${CMAKE_SOURCE_DIR}/include/pingcap DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/" FILES_MATCHING PATTERN "*.h")

if (ENABLE_TESTS)
    set(BUILD_GMOCK OFF)
    add_subdirectory (test)
endif()
