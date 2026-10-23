cmake_minimum_required(VERSION 3.16)
project(raft_lsm_store CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

option(RAFTKV_BUILD_NETWORK "Build gRPC network layer" OFF)

include(FetchContent)
FetchContent_Declare(googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
FetchContent_MakeAvailable(googletest)

enable_testing()
add_subdirectory(src/storage)
add_subdirectory(src/raft)
if(RAFTKV_BUILD_NETWORK)
  add_subdirectory(src/network)
endif()
add_subdirectory(tests)
