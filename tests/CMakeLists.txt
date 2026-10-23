include(GoogleTest)

add_executable(smoke_test smoke_test.cpp)
target_include_directories(smoke_test PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(smoke_test GTest::gtest_main)
gtest_discover_tests(smoke_test)

# Any *_test.cpp in tests/storage/ is picked up automatically.
file(GLOB STORAGE_TESTS CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/storage/*_test.cpp)
add_executable(storage_tests ${STORAGE_TESTS})
target_link_libraries(storage_tests raftkv_storage GTest::gtest_main)
gtest_discover_tests(storage_tests)

# Any *_test.cpp in tests/raft/ is picked up automatically.
file(GLOB RAFT_TESTS CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/raft/*_test.cpp)
add_executable(raft_tests ${RAFT_TESTS})
target_link_libraries(raft_tests raftkv_raft GTest::gtest_main)
gtest_discover_tests(raft_tests)

if(RAFTKV_BUILD_NETWORK)
  add_executable(network_proto_tests network/proto_conversions_test.cpp)
  target_link_libraries(network_proto_tests raftkv_network GTest::gtest_main)
  gtest_discover_tests(network_proto_tests DISCOVERY_MODE PRE_TEST)
endif()
