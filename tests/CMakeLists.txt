add_executable(smoke_test smoke_test.cpp)
target_link_libraries(smoke_test GTest::gtest_main)
include(GoogleTest)
gtest_discover_tests(smoke_test)
