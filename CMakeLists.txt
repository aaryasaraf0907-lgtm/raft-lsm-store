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

# Storage benchmarks (benchmarks/): off by default, not run by ctest or CI; use a Release build.
#   storage_read_bench   point reads and scans (and the optional block cache)
#   storage_write_bench  fill, overwrite and write amplification
option(RAFTKV_BUILD_BENCHMARKS "Build the storage benchmarks" OFF)
if(RAFTKV_BUILD_BENCHMARKS)
  foreach(bench storage_read_bench storage_write_bench)
    add_executable(${bench} benchmarks/${bench}.cpp)
    target_link_libraries(${bench} raftkv_storage)
    target_compile_definitions(${bench} PRIVATE RAFTKV_BENCH_BUILD_TYPE="$<CONFIG>")
    set_target_properties(${bench} PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/benchmarks)
  endforeach()
endif()
