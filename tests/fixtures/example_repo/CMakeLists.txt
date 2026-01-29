cmake_minimum_required(VERSION 3.16)
project(VLFSExample)

# Example integration
include(${CMAKE_CURRENT_LIST_DIR}/../../../VLFSSync.cmake)

# Optional configure-time sync
set(VLFSSYNC_AUTO OFF)
vlfs_sync()

add_executable(example main.cpp)
add_dependencies(example vfs-sync)
