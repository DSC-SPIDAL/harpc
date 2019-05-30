cmake_minimum_required(VERSION 3.12)
project(harp_data_structures)

include_directories(${MPI_INCLUDE_PATH})

add_library(
        harp_data_structures STATIC
        TableInfo.cpp
        CSRGraph.cpp
)

target_include_directories(harp_data_structures PUBLIC
        ${MPI_INCLUDE_PATH}
        ../utils
        ../thirdparty
        )

