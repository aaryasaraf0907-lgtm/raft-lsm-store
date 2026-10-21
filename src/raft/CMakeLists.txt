find_package(Threads REQUIRED)
# Any new .cpp file in this folder is picked up automatically.
file(GLOB RAFT_SOURCES CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)
add_library(raftkv_raft ${RAFT_SOURCES})
target_include_directories(raftkv_raft PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(raftkv_raft PUBLIC Threads::Threads)
