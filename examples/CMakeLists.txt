cmake_minimum_required(VERSION 3.12)
project(harp_examples)

add_executable(harp_examples
        subgraph/sc-main.cpp
        subgraph/CountMat.cpp
        subgraph/DataTableColMajor.cpp
        subgraph/DivideTemplates.cpp
        subgraph/EdgeList.cpp
        subgraph/Graph.cpp
        subgraph/IndexSys.cpp
        )

# target_compile_definitions (harp_examples PRIVATE MKL_ILP64)
# target_compile_options(harp_examples PRIVATE -I${MKLROOT}/include)

target_include_directories(harp_examples PUBLIC
        ../data_structures
        ../utils
        ../thirdparty
        )

target_link_libraries(
        harp_examples PUBLIC
        harp_data_structures
        harp_utils
        harp_thirdparty
        ${MPI_C_LIBRARIES}
        ${Boost_LIBRARIES}
        ${BLAS_LIBRARIES}
        OpenMP::OpenMP_CXX
)


FILE(GLOB PUBLIC_HEADERS *.h)

INSTALL(FILES ${PUBLIC_HEADERS}
        DESTINATION include/examples)

INSTALL(
        TARGETS harp_examples
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        PUBLIC_HEADER DESTINATION include
)

