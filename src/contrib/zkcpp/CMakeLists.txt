#Specify the version being used aswell as the language
cmake_minimum_required(VERSION 2.6)
#Name your project here
project(zkcpp)
find_package(Threads)
find_package(Boost COMPONENTS thread REQUIRED)


include_directories(
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

file(GLOB test
  ${PROJECT_SOURCE_DIR}/tests/*.cc
  ${PROJECT_SOURCE_DIR}/tests/*.c
  ${PROJECT_SOURCE_DIR}/tests/*.h
)

find_path(CPPUNIT_INCLUDE_DIR cppunit/Test.h)
find_library(CPPUNIT_LIBRARY NAMES cppunit)

IF (CPPUNIT_INCLUDE_DIR AND CPPUNIT_LIBRARY)
   SET(CPPUNIT_FOUND TRUE)
ENDIF (CPPUNIT_INCLUDE_DIR AND CPPUNIT_LIBRARY)

IF (CPPUNIT_FOUND)
   # show which CppUnit was found only if not quiet
   IF (NOT CppUnit_FIND_QUIETLY)
      MESSAGE(STATUS "Found CppUnit: ${CPPUNIT_LIBRARY}")
   ENDIF (NOT CppUnit_FIND_QUIETLY)

ELSE (CPPUNIT_FOUND)

   # fatal error if CppUnit is required but not found
   IF (CppUnit_FIND_REQUIRED)
      MESSAGE(FATAL_ERROR "Could not find CppUnit")
   ENDIF (CppUnit_FIND_REQUIRED)

ENDIF (CPPUNIT_FOUND)

set_source_files_properties(${source} ${test} PROPERTIES LANGUAGE CXX)
add_definitions(
  -DTHREADED
  -DZKSERVER_CMD="${PROJECT_SOURCE_DIR}/tests/zkServer.sh"
)

add_library(zkcpp SHARED ${header} ${source})
target_link_libraries (zkcpp ${CMAKE_THREAD_LIBS_INIT})
set(CMAKE_EXE_LINKER_FLAGS
"-Wl,--wrap -Wl,calloc -Wl,--wrap -Wl,free -Wl,--wrap -Wl,flush_send_queue -Wl,--wrap -Wl,get_xid -Wl,--wrap -Wl,deliverWatchers -Wl,--wrap -Wl,activateWatcher -Wl,--wrap -Wl,pthread_mutex_lock -Wl,--wrap -Wl,pthread_mutex_trylock -Wl,--wrap -Wl,pthread_mutex_unlock"
)



message(STATUS ${CMAKE_EXE_LINKER_FLAGS})
add_executable(zktest ${test})
target_link_libraries(zktest ${CPPUNIT_LIBRARY}   ${Boost_THREAD_LIBRARY}
)
target_link_libraries(zktest zkcpp)
