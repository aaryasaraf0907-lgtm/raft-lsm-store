find_package(Threads REQUIRED)

add_library(raftkv_kv_client kv_client.cpp)
target_include_directories(raftkv_kv_client PUBLIC "${CMAKE_SOURCE_DIR}/src")
target_link_libraries(raftkv_kv_client PUBLIC raftkv_network Threads::Threads)

add_executable(raftkv_cli cli_main.cpp)
target_link_libraries(raftkv_cli PRIVATE raftkv_kv_client)
set_target_properties(raftkv_cli PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
