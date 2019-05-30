cmake_minimum_required(VERSION 3.12)
project(harp_examples)

# whether distributed version will be compiled
# comment out these lines for non-idistributed
set(DISTRI yes)
set(CMAKE_CXX_FLAGS "-DDISTRI ${CMAKE_CXX_FLAGS}")

# whether it will be verbose
# comment out these lines for non-verbose
set(VERBOSE yes)
set(CMAKE_CXX_FLAGS "-DVERBOSE ${CMAKE_CXX_FLAGS}")

add_executable(harp_examples
        subgraph/sc-main.cpp
        subgraph/CountMat.cpp
        subgraph/DataTableColMajor.cpp
        subgraph/DivideTemplates.cpp
        subgraph/EdgeList.cpp
        subgraph/Graph.cpp
        subgraph/IndexSys.cpp
        )

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

