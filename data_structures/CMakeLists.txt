cmake_minimum_required(VERSION 3.12)
project(harp_data_structures)

include_directories(${MPI_INCLUDE_PATH})

add_library(
        harp_data_structures STATIC
        TableInfo.cpp)

target_include_directories(harp_data_structures PUBLIC
        ${MPI_INCLUDE_PATH}
        ../utils
        ../thirdparty
        )


#target_link_libraries(
#        harp_data_structures INTERFACE
#        ${MPI_C_LIBRARIES}
#)

FILE(GLOB PUBLIC_HEADERS *.h)

INSTALL(FILES ${PUBLIC_HEADERS}
        DESTINATION include/data_structures)

INSTALL(
        TARGETS harp_data_structures
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib
)