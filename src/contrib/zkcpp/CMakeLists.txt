cmake_minimum_required(VERSION 2.6)
project(zkcpp)
include(CheckTypeSize)

include_directories(
  ${CMAKE_CURRENT_BINARY_DIR}
  ${PROJECT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/include
  ${PROJECT_SOURCE_DIR}/generated
  ${PROJECT_SOURCE_DIR}/src
  ${PROJECT_SOURCE_DIR}/src/hashtable
  ${PROJECT_SOURCE_DIR}/tests
)

file(GLOB header
  ${PROJECT_SOURCE_DIR}/include/*h
  ${PROJECT_SOURCE_DIR}/generated/*h
  ${PROJECT_SOURCE_DIR}/src/*h
  ${PROJECT_SOURCE_DIR}/src/hashtable/*h
)

file(GLOB source
  ${PROJECT_SOURCE_DIR}/src/*.cc
  ${PROJECT_SOURCE_DIR}/src/*.c
  ${PROJECT_SOURCE_DIR}/src/hashtable/*.c
  ${PROJECT_SOURCE_DIR}/generated/*.c
)

# TODO derive this from ${source} and ${testsrc}
file(GLOB testsrc
  ${PROJECT_SOURCE_DIR}/src/*.cc
  ${PROJECT_SOURCE_DIR}/src/*.c
  ${PROJECT_SOURCE_DIR}/src/hashtable/*.c
  ${PROJECT_SOURCE_DIR}/generated/*.c
  ${PROJECT_SOURCE_DIR}/tests/*.cc
  ${PROJECT_SOURCE_DIR}/tests/*.c
  ${PROJECT_SOURCE_DIR}/tests/*.h
)

# Set parameters needed by config.h.in to generate config.h
set(PACKAGE_STRING "\"ZooKeeper C++ Client 3.5.0\"")

# Check if ndfs_t type is defined in poll.h. If it is not defined, use
# unsigned int.
set(CMAKE_EXTRA_INCLUDE_FILES poll.h)
check_type_size("nfds_t" NFDS_T_SIZE)
set(CMAKE_EXTRA_INCLUDE_FILES)
if(NFDS_T_SIZE)
  set(POLL_NFDS_TYPE nfds_t)
else(NFDS_T_SIZE)
set(POLL_NFDS_TYPE "unsigned int")
endif(NFDS_T_SIZE)
configure_file(
  ${PROJECT_SOURCE_DIR}/config.h.in
  ${CMAKE_CURRENT_BINARY_DIR}/config.h
)

# Look for threading library (e.g. pthread)
find_package(Threads)

# Look for boost with thread
find_package(Boost COMPONENTS thread REQUIRED)

# Look for cppunit
find_path(CPPUNIT_INCLUDE_DIR cppunit/Test.h)
find_library(CPPUNIT_LIBRARY NAMES cppunit)

if(CPPUNIT_INCLUDE_DIR AND CPPUNIT_LIBRARY)
  set(CPPUNIT_FOUND TRUE)
endif(CPPUNIT_INCLUDE_DIR AND CPPUNIT_LIBRARY)

if(CPPUNIT_FOUND)
  message(STATUS "Found CppUnit: ${CPPUNIT_LIBRARY}")
else(CPPUNIT_FOUND)
  message(FATAL_ERROR "Could not find CppUnit")
endif(CPPUNIT_FOUND)

# Treat C source code as C++ source code.
set_source_files_properties(${source} ${testsrc} PROPERTIES LANGUAGE CXX)

add_definitions(
  -DTHREADED
  -DZKSERVER_CMD="${PROJECT_SOURCE_DIR}/tests/zkServer.sh"
)

add_library(zkcpp SHARED ${header} ${source})
target_link_libraries (zkcpp ${CMAKE_THREAD_LIBS_INIT})
set(CMAKE_EXE_LINKER_FLAGS
"-Wl,--wrap -Wl,calloc -Wl,--wrap -Wl,free -Wl,--wrap -Wl,flush_send_queue -Wl,--wrap -Wl,get_xid -Wl,--wrap -Wl,deliverWatchers -Wl,--wrap -Wl,activateWatcher -Wl,--wrap -Wl,pthread_mutex_lock -Wl,--wrap -Wl,pthread_mutex_trylock -Wl,--wrap -Wl,pthread_mutex_unlock"
)

add_executable(zktest ${testsrc})
target_link_libraries(zktest ${CPPUNIT_LIBRARY}   ${Boost_THREAD_LIBRARY}
)

# Add "make lint" target
add_custom_target(lint ${PROJECT_SOURCE_DIR}/cpplint.py ${testsrc} ${header})
