cmake_minimum_required(VERSION 2.6)
project(zkcpp)
include(CheckTypeSize)

include_directories(
  ${CMAKE_CURRENT_BINARY_DIR}
  ${PROJECT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/include
  ${PROJECT_SOURCE_DIR}/generated
  ${PROJECT_SOURCE_DIR}/src
  ${PROJECT_SOURCE_DIR}/tests
)

file(GLOB header
  ${PROJECT_SOURCE_DIR}/include/*h
  ${PROJECT_SOURCE_DIR}/generated/*.h
  ${PROJECT_SOURCE_DIR}/generated/*.hh
  ${PROJECT_SOURCE_DIR}/src/*h
)

file(GLOB source
  ${PROJECT_SOURCE_DIR}/src/*.cc
  ${PROJECT_SOURCE_DIR}/src/*.c
  ${PROJECT_SOURCE_DIR}/generated/*.c
  ${PROJECT_SOURCE_DIR}/generated/*.cc
)

# TODO derive this from ${source} and ${testsrc}
file(GLOB testsrc
  ${PROJECT_SOURCE_DIR}/src/*.cc
  ${PROJECT_SOURCE_DIR}/src/*.c
  ${PROJECT_SOURCE_DIR}/generated/*.c
  ${PROJECT_SOURCE_DIR}/generated/*.cc
  ${PROJECT_SOURCE_DIR}/tests/TestDriver.cc
  ${PROJECT_SOURCE_DIR}/tests/MocksBase.cc
  ${PROJECT_SOURCE_DIR}/tests/LibCSymTable.cc
  ${PROJECT_SOURCE_DIR}/tests/LibCMocks.cc
  ${PROJECT_SOURCE_DIR}/tests/Util.cc
  ${PROJECT_SOURCE_DIR}/tests/TestClientRetry.cc
  ${PROJECT_SOURCE_DIR}/tests/TestZookeeperClose.cc
  #${PROJECT_SOURCE_DIR}/tests/TestWatchers.cc
  ${PROJECT_SOURCE_DIR}/tests/ThreadingUtil.cc
  ${PROJECT_SOURCE_DIR}/tests/TestRecordIo.cc
  ${PROJECT_SOURCE_DIR}/tests/TestZookeeperInit.cc
  ${PROJECT_SOURCE_DIR}/tests/TestMulti.cc
  ${PROJECT_SOURCE_DIR}/tests/TestOp.cc
  #${PROJECT_SOURCE_DIR}/tests/TestOperations.cc
  ${PROJECT_SOURCE_DIR}/tests/ZKMocks.cc
  ${PROJECT_SOURCE_DIR}/tests/TestCppClient.cc
  #${PROJECT_SOURCE_DIR}/tests/TestClient.cc
  ${PROJECT_SOURCE_DIR}/tests/*.h
)

# Set parameters needed by config.h.in to generate config.h
set(PACKAGE_STRING "\"ZooKeeper C++ Client 3.5.0\"")

if (CMAKE_COMPILER_IS_GNUCXX)
  set(CMAKE_CXX_FLAGS "-g -Wall")
endif()

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

# Look for log4cpp
find_path(LOG4CXX_INCLUDE_DIR log4cxx/log4cxx.h)
find_library(LOG4CXX_LIBRARY NAMES log4cxx)

if(LOG4CXX_INCLUDE_DIR AND LOG4CXX_LIBRARY)
  set(LOG4CXX_FOUND TRUE)
endif()

if(LOG4CXX_FOUND)
  message(STATUS "Found log4cxx: ${LOG4CXX_LIBRARY}")
else()
  message(FATAL_ERROR "Could not find log4cxx")
endif()


# Look for boost with thread
find_package(Boost COMPONENTS thread REQUIRED)
find_package(Boost COMPONENTS system REQUIRED)


# Look for cppunit
find_path(CPPUNIT_INCLUDE_DIR cppunit/Test.h)
find_library(CPPUNIT_LIBRARY NAMES cppunit)

if(CPPUNIT_INCLUDE_DIR AND CPPUNIT_LIBRARY)
  set(CPPUNIT_FOUND TRUE)
endif()

if(CPPUNIT_FOUND)
  message(STATUS "Found cppunit: ${CPPUNIT_LIBRARY}")
else()
  message(FATAL_ERROR "Could not find cppunit")
endif()

# Treat C source code as C++ source code.
set_source_files_properties(${source} ${testsrc} PROPERTIES LANGUAGE CXX)

add_definitions(
  -DZKSERVER_CMD="${PROJECT_SOURCE_DIR}/tests/zkServer.sh"
)

add_library(zkcpp SHARED ${header} ${source})
target_link_libraries (zkcpp
  ${LOG4CXX_LIBRARY}
)

set(CMAKE_EXE_LINKER_FLAGS
"-Wl,--wrap -Wl,calloc -Wl,--wrap -Wl,free -Wl,--wrap -Wl,flush_send_queue -Wl,--wrap -Wl,get_xid -Wl,--wrap -Wl,deliverWatchers -Wl,--wrap -Wl,activateWatcher"
)

add_executable(zktest ${testsrc})
target_link_libraries(zktest
  ${CPPUNIT_LIBRARY}
  ${Boost_THREAD_LIBRARY}
  ${Boost_SYSTEM_LIBRARY}
  ${LOG4CXX_LIBRARY}
)

# Add "make lint" target
add_custom_target(lint ${PROJECT_SOURCE_DIR}/cpplint.py ${testsrc} ${header})
file(COPY ${PROJECT_SOURCE_DIR}/tests/log4cxx.properties DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
