cmake_minimum_required(VERSION 3.12)
project(harp_worker)

include_directories(
        ${MPI_INCLUDE_PATH}
)

add_library(harp_worker Worker.cpp)


FILE(GLOB PUBLIC_HEADERS *.h)

INSTALL(FILES ${PUBLIC_HEADERS}
        DESTINATION include/worker)

INSTALL(
        TARGETS harp_worker
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib
)