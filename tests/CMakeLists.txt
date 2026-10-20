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
