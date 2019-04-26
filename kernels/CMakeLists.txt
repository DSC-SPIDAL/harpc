cmake_minimum_required(VERSION 3.12)
project(harp_kernels)

add_library(
        harp_kernels INTERFACE
)

FILE(GLOB PUBLIC_HEADERS *.h)

INSTALL(FILES ${PUBLIC_HEADERS}
        DESTINATION include/kernels)

INSTALL(
        TARGETS harp_kernels
        LIBRARY DESTINATION lib
)