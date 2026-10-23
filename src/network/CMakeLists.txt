
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

set(RAFTKV_PROTO_FILE
    "${CMAKE_CURRENT_SOURCE_DIR}/proto/raft_rpc.proto")

set(RAFTKV_PROTO_SOURCES
    "${CMAKE_CURRENT_BINARY_DIR}/raft_rpc.pb.cc")
set(RAFTKV_PROTO_HEADERS
    "${CMAKE_CURRENT_BINARY_DIR}/raft_rpc.pb.h")

add_custom_command(
    OUTPUT
        "${RAFTKV_PROTO_SOURCES}"
        "${RAFTKV_PROTO_HEADERS}"
    COMMAND $<TARGET_FILE:protobuf::protoc>
    ARGS
        "--cpp_out=${CMAKE_CURRENT_BINARY_DIR}"
        "--proto_path=${CMAKE_CURRENT_SOURCE_DIR}/proto"
        "${RAFTKV_PROTO_FILE}"
    DEPENDS
        "${RAFTKV_PROTO_FILE}"
    COMMENT "Generating C++ sources from raft_rpc.proto"
    VERBATIM
)

add_library(raftkv_network
    proto_conversions.cpp
    ${RAFTKV_PROTO_SOURCES}
    ${RAFTKV_PROTO_HEADERS}
)

target_include_directories(raftkv_network PUBLIC
    "${CMAKE_SOURCE_DIR}/src"
    "${CMAKE_CURRENT_BINARY_DIR}"
)

target_link_libraries(raftkv_network PUBLIC
    protobuf::libprotobuf
    gRPC::grpc++
)
