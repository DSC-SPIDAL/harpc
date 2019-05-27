//
// Created by auyar on 06.05.2019.
//

#include "Worker.h"

using namespace std;
using namespace harp;

namespace harp::worker {

    void Worker::init(int argc, char *argv[]) {
        int provided;
        MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
        if (provided < MPI_THREAD_MULTIPLE) {
            printf("ERROR: The MPI library does not have full thread support\n");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        //MPI_Init(NULL, NULL);

        MPI_Comm_size(MPI_COMM_WORLD, &this->worldSize);

        MPI_Comm_rank(MPI_COMM_WORLD, &this->workerId);

        this->initialized = true;
    }

    void Worker::start(int argc, char *argv[]) {
        if (!this->initialized) {
            printf("Worker should be initialized");
            return;
        }
        comm::Communicator comm(this->workerId, this->worldSize, this->comThreads);
        this->execute(&comm, argc, argv);
        MPI_Finalize();
    }

    void Worker::setCommThreads(int comThreads) {
        this->comThreads = comThreads;
    }

    bool Worker::isMaster() {
        return this->workerId == 0;
    }

} // end of namespace harp::worker