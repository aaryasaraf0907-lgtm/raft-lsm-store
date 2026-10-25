find_package(Threads REQUIRED)

add_library(raftkv_node_runtime node_runtime.cpp)
target_include_directories(raftkv_node_runtime PUBLIC "${CMAKE_SOURCE_DIR}/src")
target_link_libraries(raftkv_node_runtime PUBLIC
    raftkv_client_service
    raftkv_network
    raftkv_raft
    raftkv_storage
    Threads::Threads)

add_executable(raftkv_node node_main.cpp)
target_link_libraries(raftkv_node PRIVATE raftkv_node_runtime)
set_target_properties(raftkv_node PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
