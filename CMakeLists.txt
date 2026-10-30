cmake_minimum_required(VERSION 3.16)
project(raft_lsm_store CXX)

if(WIN32)
  message(FATAL_ERROR "RaftKV supports Linux and macOS only. On Windows, build inside WSL2 Ubuntu.")
endif()

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
  add_subdirectory(src/cli)
  add_subdirectory(src/node)
endif()
add_subdirectory(tests)

# Read-path micro-benchmark (benchmarks/storage_read_bench.cpp). Off by default; use a Release build.
option(RAFTKV_BUILD_BENCHMARKS "Build the storage read benchmark" OFF)
if(RAFTKV_BUILD_BENCHMARKS)
  add_executable(storage_read_bench benchmarks/storage_read_bench.cpp)
  target_link_libraries(storage_read_bench raftkv_storage)
  set_target_properties(storage_read_bench PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/benchmarks)
endif()
