
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

set(RAFTKV_PROTO_FILE
    "${CMAKE_CURRENT_SOURCE_DIR}/proto/raft_rpc.proto")

set(RAFTKV_PROTO_SOURCES
    "${CMAKE_CURRENT_BINARY_DIR}/raft_rpc.pb.cc")
set(RAFTKV_PROTO_HEADERS
    "${CMAKE_CURRENT_BINARY_DIR}/raft_rpc.pb.h")
set(RAFTKV_GRPC_SOURCES
    "${CMAKE_CURRENT_BINARY_DIR}/raft_rpc.grpc.pb.cc")
set(RAFTKV_GRPC_HEADERS
    "${CMAKE_CURRENT_BINARY_DIR}/raft_rpc.grpc.pb.h")

add_custom_command(
    OUTPUT
        "${RAFTKV_PROTO_SOURCES}"
        "${RAFTKV_PROTO_HEADERS}"
        "${RAFTKV_GRPC_SOURCES}"
        "${RAFTKV_GRPC_HEADERS}"
    COMMAND $<TARGET_FILE:protobuf::protoc>
    ARGS
        "--cpp_out=${CMAKE_CURRENT_BINARY_DIR}"
        "--grpc_out=${CMAKE_CURRENT_BINARY_DIR}"
        "--plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
        "--proto_path=${CMAKE_CURRENT_SOURCE_DIR}/proto"
        "${RAFTKV_PROTO_FILE}"
    DEPENDS
        "${RAFTKV_PROTO_FILE}"
        protobuf::protoc
        gRPC::grpc_cpp_plugin
    COMMENT "Generating C++ sources from raft_rpc.proto"
    VERBATIM
)

add_library(raftkv_network
    grpc_transport.cpp
    proto_conversions.cpp
    ${RAFTKV_PROTO_SOURCES}
    ${RAFTKV_PROTO_HEADERS}
    ${RAFTKV_GRPC_SOURCES}
    ${RAFTKV_GRPC_HEADERS}
)

target_include_directories(raftkv_network PUBLIC
    "${CMAKE_SOURCE_DIR}/src"
    "${CMAKE_CURRENT_BINARY_DIR}"
)

target_link_libraries(raftkv_network PUBLIC
    protobuf::libprotobuf
    gRPC::grpc++
)
