# Any new .cpp file in this folder is picked up automatically.
file(GLOB STORAGE_SOURCES CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)
add_library(raftkv_storage ${STORAGE_SOURCES})
target_include_directories(raftkv_storage PUBLIC ${CMAKE_SOURCE_DIR}/src)
