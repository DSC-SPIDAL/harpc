cmake_minimum_required(VERSION 3.12)
project(harp_utils)


add_library(harp_utils STATIC
        Helper.cpp
        )

target_include_directories(harp_utils PUBLIC
        ../data_structures
        )


FILE(GLOB PUBLIC_HEADERS *.h)

INSTALL(FILES ${PUBLIC_HEADERS}
        DESTINATION include/utils)

INSTALL(
        TARGETS harp_utils
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib
)