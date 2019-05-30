cmake_minimum_required(VERSION 3.12)
project(harp_utils)


add_library(harp_utils STATIC
        Helper.cpp
        )

target_include_directories(harp_utils PUBLIC
        ../data_structures
        )

