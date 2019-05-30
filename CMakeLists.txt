cmake_minimum_required(VERSION 3.12)

project(harpc)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "-std=c++17  ${CMAKE_CXX_FLAGS}")

#Boost
set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_RUNTIME OFF)
find_package(Boost 1.65.1 COMPONENTS thread filesystem system REQUIRED)
#find_package(Boost 1.68.0 COMPONENTS thread REQUIRED)

#MPI
find_package(MPI REQUIRED)

# MKL
# MKL goes with another name
set(BLA_VENDOR Intel10_64lp)
find_package(BLAS REQUIRED)

# for intel AVX2, use: -xCORE-AVX2
# for AVX512 use: -xCORE-AVX512
# for knl, use: -xMIC-AVX512
set(CMAKE_CXX_FLAGS "-xCORE-AVX2 ${CMAKE_CXX_FLAGS}")
set(CMAKE_C_FLAGS   "-xCORE-AVX2 ${CMAKE_C_FLAGS}")

# MKL flags
set(CMAKE_CXX_FLAGS "-L${MKLROOT}/lib/intel64 -lmkl_intel_lp64 -lmkl_intel_thread -lmkl_core ${CMAKE_CXX_FLAGS}")
set(CMAKE_C_FLAGS   "-L${MKLROOT}/lib/intel64 -lmkl_intel_lp64 -lmkl_intel_thread -lmkl_core ${CMAKE_C_FLAGS}")

# OpenMP
find_package(OpenMP REQUIRED)
set(CMAKE_CXX_FLAGS " -qopenmp ${CMAKE_CXX_FLAGS}")
set(CMAKE_C_FLAGS " -qopenmp ${CMAKE_C_FLAGS}")

include_directories(
        ${PROJECT_BINARY_DIR}
        ${Boost_INCLUDE_DIRS}
        ${MPI_INCLUDE_PATH}
        ${MKL_INCLUDE_PATH}
)

add_subdirectory(communication)
add_subdirectory(data_structures)
add_subdirectory(kernels)
add_subdirectory(utils)
add_subdirectory(worker)
add_subdirectory(thirdparty)
add_subdirectory(examples)


#add_executable(harpc
#        main.cpp
#        examples/subgraph/sc-main.cpp
#        examples/kmeans/KMeansWorker.cpp
#        examples/generators/DataGenerator.cpp
#        examples/generators/DataGenerator.h
#        )

#kernels/daal/DaalKMeans.cpp
#examples/kmeans/kmeans.cpp

#find_library(DAAL_CORE_LIB NAMES daal_core PATHS ${DAAL_LIBRARIES})
#find_library(DAAL_THREAD_LIB NAMES daal_thread PATHS ${DAAL_LIBRARIES})
#find_library(DAAL_SEQ_LIB NAMES daal_sequential PATHS ${DAAL_LIBRARIES})
#find_library(TBB_LIB NAMES tbb tbbmalloc PATHS ${TBB_LIBRARIES})

#message("Daal Core Lib : " ${DAAL_CORE_LIB})
#message("Daal Thread Lib :" ${DAAL_THREAD_LIB})
#message("Daal Sequence Lib : " ${DAAL_SEQ_LIB})

#target_include_directories(
#        harpc PUBLIC
#        data_structures
#        utils
#        worker
#        communication
#        kernels
#        thirdparty
#        thirdparty/graph500-1.2
#        thirdparty/SpMP
#)
