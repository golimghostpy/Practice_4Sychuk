cmake_minimum_required(VERSION 3.0)
project(.)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/bin)
include_directories(.)

add_executable(pr4
        libs.h
        main.cpp
        structures.h
        structures.cpp
)
