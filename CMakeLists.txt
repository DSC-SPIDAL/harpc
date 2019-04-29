cmake_minimum_required(VERSION 3.12)

#SET(CMAKE_C_COMPILER /home/chathura/intel/compilers_and_libraries_2019.1.144/linux/bin/intel64/icc)
#SET(CMAKE_CXX_COMPILER /home/chathura/intel/compilers_and_libraries_2019.1.144/linux/bin/intel64/icpc)

project(harpc)

set(CMAKE_CXX_STANDARD 17)
#set(CMAKE_CXX_STANDARD 14)

list(APPEND CMAKE_CXX_FLAGS "-pthread -std=c++11 ${CMAKE_CXX_FLAGS}")

set(INSTALL_LOCATION /home/auyar/projects/harp/tmp)

#Boost
set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_RUNTIME OFF)
find_package(Boost 1.65.1 COMPONENTS thread filesystem system REQUIRED)
#find_package(Boost 1.68.0 COMPONENTS thread REQUIRED)

#MPI
SET(MPI_C_COMPILER /opt/openmpi-4.0.1/build/bin/mpicc)
SET(MPI_CXX_COMPILER /opt/openmpi-4.0.1/build/bin/mpicxx)
find_package(MPI REQUIRED)

#DAAL
#SET(DAAL_INCLUDE_PATH /opt/l_daal_oss_p_2019.3.006/daal_prebuild/linux/daal/include)
#SET(DAAL_LIBRARIES /opt/l_daal_oss_p_2019.3.006/daal_prebuild/linux/daal/lib/intel64_lin)

include_directories(
        ${PROJECT_BINARY_DIR}
        ${Boost_INCLUDE_DIRS}
        ${MPI_INCLUDE_PATH}
#        ${DAAL_INCLUDE_PATH}
)

add_subdirectory(communication)
add_subdirectory(data_structures)
add_subdirectory(kernels)
add_subdirectory(util)
add_subdirectory(worker)

add_executable(harpc examples/kmeans/KMeansWorker.cpp
        examples/generators/DataGenerator.cpp
        examples/generators/DataGenerator.h
        )

#kernels/daal/DaalKMeans.cpp
#examples/kmeans/kmeans.cpp

#find_library(DAAL_CORE_LIB NAMES daal_core PATHS ${DAAL_LIBRARIES})
#find_library(DAAL_THREAD_LIB NAMES daal_thread PATHS ${DAAL_LIBRARIES})
#find_library(DAAL_SEQ_LIB NAMES daal_sequential PATHS ${DAAL_LIBRARIES})
#find_library(TBB_LIB NAMES tbb tbbmalloc PATHS ${TBB_LIBRARIES})

#message("Daal Core Lib : " ${DAAL_CORE_LIB})
#message("Daal Thread Lib :" ${DAAL_THREAD_LIB})
#message("Daal Sequence Lib : " ${DAAL_SEQ_LIB})

target_include_directories(
        harpc PUBLIC
        data_structures
        util
        worker
        communication
        kernels
)

target_link_libraries(
        harpc
        ${MPI_C_LIBRARIES}
        ${Boost_LIBRARIES}
        #        ${DAAL_CORE_LIB}
        #        ${DAAL_THREAD_LIB}
        #        ${DAAL_SEQ_LIB}
)

message("Installing to : " ${INSTALL_LOCATION})

set(CMAKE_INSTALL_PREFIX ${INSTALL_LOCATION})

INSTALL(FILES harp.h
        DESTINATION include)

INSTALL(
        TARGETS harpc
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        PUBLIC_HEADER DESTINATION include
)