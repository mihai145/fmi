cmake_minimum_required(VERSION 3.10)
project(fmi)
set(CMAKE_CXX_STANDARD 17)

# fmi is shared library
add_compile_definitions(BOOST_LOG_DYN_LINK)

if(FMI_TARGET_ALREADY_DEFINED)
    message(STATUS "FMI target has already been defined")
else()
    message(STATUS "Adding FMI subdirectory")
    add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/.." libfmi)
endif()

find_package(Boost COMPONENTS system python3 REQUIRED)
find_package(Python3 COMPONENTS Interpreter Development)

set(CMAKE_SHARED_MODULE_PREFIX "")
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

add_library(fmi MODULE fmi_python.cpp PythonCommunicator.cpp)

target_include_directories(fmi PRIVATE ${Python3_INCLUDE_DIRS} ${FMI_INCLUDE_DIRS})

target_link_libraries(fmi Boost::python3 ${Python3_LIBRARIES} FMI)
