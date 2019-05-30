cmake_minimum_required(VERSION 3.12)
project(harp_worker)

add_library(harp_worker STATIC Worker.cpp)

target_include_directories(harp_worker PUBLIC
        ${MPI_INCLUDE_PATH}
)

