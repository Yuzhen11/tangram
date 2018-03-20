# Copyright 2017 Husky Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


project(xyz)
cmake_minimum_required(VERSION 3.0.2)

# CMake Options
set(CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})
if(CMAKE_COMPILER_IS_GNUCXX)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -DHUSKY_DEBUG_MODE")
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -O2 -g -DHUSKY_DEBUG_MODE")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -w")
    set(CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS} "-Wno-deprecated-declarations")
endif()

# enable this by `cmake .. -DUSE_PROFILER=ON`
option(USE_PROFILER "Use profiler" OFF)
if(USE_PROFILER)
    add_definitions(-DUSE_PROFILER)
    message("Compiled with USE_PROFILER on")
endif()

find_package(Threads)
find_package(Boost 1.58.0 COMPONENTS thread)

# External Dependencies
include(cmake/modules/glog.cmake)
include(cmake/modules/gflags.cmake)
include(cmake/modules/gtest.cmake)
include(cmake/modules/zeromq.cmake)
include(cmake/dep.cmake)

# External Includes
set(HUSKY_EXTERNAL_INCLUDE ${ZMQ_INCLUDE_DIR} ${GLOG_INCLUDE_DIR} ${GFLAGS_INCLUDE_DIR} ${Boost_INCLUDE_DIRS})

# External Libraries
set(HUSKY_EXTERNAL_LIB ${GLOG_LIBRARY} ${GFLAGS_LIBRARY} ${ZMQ_LIBRARY} ${CMAKE_THREAD_LIBS_INIT} ${Boost_LIBRARIES})

if(LIBHDFS3_FOUND)
    list(APPEND HUSKY_EXTERNAL_INCLUDE ${LIBHDFS3_INCLUDE_DIR})
    list(APPEND HUSKY_EXTERNAL_LIB ${LIBHDFS3_LIBRARY})
    list(APPEND HUSKY_EXTERNAL_DEFINITION ${LIBHDFS3_DEFINITION})
endif(LIBHDFS3_FOUND)

add_subdirectory(base)
add_subdirectory(core)
add_subdirectory(comm)
add_subdirectory(io)

add_library(xyz STATIC
  $<TARGET_OBJECTS:base-objs>
  $<TARGET_OBJECTS:core-objs>
  $<TARGET_OBJECTS:comm-objs>
  $<TARGET_OBJECTS:io-objs>
  )

add_subdirectory(test)
add_subdirectory(examples)
