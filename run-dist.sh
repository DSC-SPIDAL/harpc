# set intel compiler environment variables
source /home/auyar/intel/compilers_and_libraries/linux/bin/compilervars.sh -arch intel64 -platform linux
source /home/auyar/intel/compilers_and_libraries_2019.3.199/linux/mpi/intel64/bin/mpivars.sh
source /home/auyar/intel/mkl/bin/mklvars.sh intel64

# example run commands
mpiexec -np 4 build/examples/harp_examples ../harp/data/gnp.graph ../harp/data/template/u10-1.fascia 10 4 0 0

