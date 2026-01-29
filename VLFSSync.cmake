# VLFSSync.cmake - CMake integration for VLFS
# Include this file in your CMakeLists.txt:
#   include(VLFSSync.cmake)
#
# Then call vlfs_sync() or use the vfs-sync target:
#   vlfs_sync()  # Run during configure
#   add_dependencies(my_target vfs-sync)  # Run during build

option(VLFSSYNC_AUTO "Automatically run VLFS sync during CMake configure" OFF)

# Find Python executable
find_package(Python3 COMPONENTS Interpreter QUIET)
if(NOT Python3_FOUND)
    # Fallback to finding python3 or python
    find_program(PYTHON_EXECUTABLE python3 python
        DOC "Python executable for VLFS"
    )
    if(NOT PYTHON_EXECUTABLE)
        message(WARNING "VLFS: Python not found. VLFS sync will not work.")
    endif()
else()
    set(PYTHON_EXECUTABLE ${Python3_EXECUTABLE})
endif()

# Function to run VLFS sync
define_property(TARGET PROPERTY VLFS_SYNCED
    BRIEF_DOCS "Whether VLFS sync has been run for this target"
    FULL_DOCS "Internal property to track VLFS sync state"
)

function(vlfs_sync)
    # Run vlfs.py pull
    if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/vlfs.py")
        message(WARNING "VLFS: vlfs.py not found in source directory")
        return()
    endif()

    if(NOT PYTHON_EXECUTABLE)
        message(WARNING "VLFS: Python not found, skipping sync")
        return()
    endif()

    message(STATUS "VLFS: Running sync...")
    execute_process(
        COMMAND ${PYTHON_EXECUTABLE} "${CMAKE_CURRENT_SOURCE_DIR}/vlfs.py" pull
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        RESULT_VARIABLE VLFS_RESULT
        OUTPUT_VARIABLE VLFS_OUTPUT
        ERROR_VARIABLE VLFS_ERROR
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_STRIP_TRAILING_WHITESPACE
    )

    if(VLFS_RESULT EQUAL 0)
        message(STATUS "VLFS: Sync complete")
        if(VLFS_OUTPUT)
            message(STATUS "VLFS: ${VLFS_OUTPUT}")
        endif()
    else()
        message(WARNING "VLFS: Sync failed with code ${VLFS_RESULT}")
        if(VLFS_ERROR)
            message(WARNING "VLFS: ${VLFS_ERROR}")
        endif()
    endif()
endfunction()

# Custom target for explicit sync
add_custom_target(vfs-sync
    COMMAND ${PYTHON_EXECUTABLE} "${CMAKE_CURRENT_SOURCE_DIR}/vlfs.py" pull
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Syncing VLFS files..."
    VERBATIM
)

# Auto-run during configure if enabled
if(VLFSSYNC_AUTO)
    vlfs_sync()
endif()
