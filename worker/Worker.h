#ifndef HARPC_WORKER_H
#define HARPC_WORKER_H

#include "../communication/Communicator.h"

namespace harp::worker {

    class Worker {
    protected:
        int workerId;
        int worldSize;

        int comThreads = 1;//no of communication threads

        bool initialized = false;
    public:
        void init(int argc, char *argv[]) {
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

        void start(int argc = 0, char *argv[] = NULL) {
            if (!this->initialized) {
                printf("Worker should be initialized");
                return;
            }
            com::Communicator comm(this->workerId, this->worldSize, this->comThreads);
            this->execute(&comm, argc, argv);
            MPI_Finalize();
        }

        virtual void execute(com::Communicator *comm, int argc = 0, char *argv[] = NULL) = 0;

        void setCommThreads(int comThreads) {
            this->comThreads = comThreads;
        }

        bool isMaster() {
            return this->workerId == 0;
        }
    };
}

#endif //HARPC_WORKER_H
