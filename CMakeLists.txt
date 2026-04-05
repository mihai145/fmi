cmake_minimum_required(VERSION 3.10)
project(FMI)
set(CMAKE_CXX_STANDARD 17)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

option(FMI_BUILD_SHARED "Build FMI as a shared library" OFF)
option(FMI_RDMA "Enable RDMA support in FMI" OFF)
option(LINK_WITH_CHECKPOINTING_LIB "Link with the pre-built checkpointing library" OFF)

if(FMI_BUILD_SHARED)
    add_compile_definitions(BOOST_LOG_DYN_LINK)
else()
    set(Boost_USE_STATIC_LIBS   ON)
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

get_directory_property(hasParent PARENT_DIRECTORY)
if (hasParent)
    set(FMI_INCLUDE_DIRS "${CMAKE_CURRENT_SOURCE_DIR}/include/" PARENT_SCOPE)
endif()

find_package(Boost COMPONENTS log REQUIRED)
find_package(AWSSDK COMPONENTS s3 REQUIRED)
find_package(hiredis REQUIRED)

add_subdirectory(extern/TCPunch/client/)

set(FMI_SOURCES
    src/Communicator.cpp
    src/utils/Configuration.cpp
    src/comm/Channel.cpp
    src/comm/ClientServer.cpp
    src/comm/S3.cpp
    src/comm/Redis.cpp
    src/utils/ChannelPolicy.cpp
    src/comm/PeerToPeer.cpp
    src/comm/Direct.cpp
)

if(FMI_RDMA)
    list(APPEND FMI_SOURCES src/comm/Rdma.cpp)
    message(STATUS "Building FMI with RDMA")
else()
    message(STATUS "Building FMI without RDMA")
endif()

if(FMI_BUILD_SHARED)
    add_library(FMI SHARED ${FMI_SOURCES})
    message(STATUS "Building FMI as shared lib")
else()
    message(STATUS "Building FMI as static lib")
    add_library(FMI STATIC ${FMI_SOURCES})
endif()

target_include_directories(FMI PRIVATE ${Boost_INCLUDE_DIRS} ${TCPUNCH_INCLUDE_DIRS})

target_link_libraries(FMI ${Boost_Libraries} Boost::log ${AWSSDK_LINK_LIBRARIES} ${HIREDIS_LIBRARIES} tcpunch)

if(FMI_RDMA)
    target_compile_definitions(FMI PRIVATE FMI_RDMA=1)
    target_include_directories(FMI PRIVATE ${RDMALIB_INCLUDE_PATH})
    target_link_libraries(FMI rdmalib cereal)
endif()

if(LINK_WITH_CHECKPOINTING_LIB)
    list(APPEND FMI_SOURCES src/comm/DirectNoHolepunch.cpp)

    get_filename_component(REPO_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../.." ABSOLUTE)
    set(GAP_RUNNER_SOURCE_DIR "${REPO_ROOT}/src")
    set(GAP_RUNNER_BUILD_DIR  "${REPO_ROOT}/build/scheduler")

    set(CHECKPOINT_LIB_PATH "${GAP_RUNNER_BUILD_DIR}/libcheckpoint.so")

    if(NOT EXISTS "${CHECKPOINT_LIB_PATH}")
        message(FATAL_ERROR "Missing ${CHECKPOINT_LIB_PATH}. Build scheduler first.")
    else()
        message(STATUS "Found libcheckpoint.")
    endif()

    add_library(libcheckpoint SHARED IMPORTED GLOBAL)
    set_target_properties(libcheckpoint PROPERTIES
        IMPORTED_LOCATION "${CHECKPOINT_LIB_PATH}"
    )
    target_include_directories(libcheckpoint INTERFACE
        "${GAP_RUNNER_SOURCE_DIR}/common"
    )

    target_compile_definitions(FMI PRIVATE LINK_WITH_CHECKPOINTING_LIB=1)
    target_link_libraries(FMI libcheckpoint)
    target_include_directories(FMI PRIVATE "${GAP_RUNNER_SOURCE_DIR}/lib")
endif()

#find_package(OpenMP REQUIRED)

#add_executable(client tests/client.cpp)

#target_link_libraries(client FMI OpenMP::OpenMP_CXX)

#add_subdirectory(tests)
