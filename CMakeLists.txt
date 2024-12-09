cmake_minimum_required(VERSION 3.29)
project(Practice_4)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY /home/golimghost/CLionProjects/Practice_4Sychuk/bin)
include_directories(.)

add_executable(pr4
        libs.h
        main.cpp
        structures.h
        structures.cpp
)
