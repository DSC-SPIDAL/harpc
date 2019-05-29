# set intel compiler, mpi and mkl environment variables
source /home/auyar/intel/compilers_and_libraries/linux/bin/compilervars.sh -arch intel64 -platform linux
source /home/auyar/intel/compilers_and_libraries_2019.3.199/linux/mpi/intel64/bin/mpivars.sh
source /home/auyar/intel/mkl/bin/mklvars.sh intel64

# set c and c++ compilers
CC=icc CXX=icpc

# compile the project
cd build
/opt/clion-2019.1/bin/cmake/linux/bin/cmake ..
make
