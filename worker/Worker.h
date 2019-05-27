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
        void init(int argc, char *argv[]);

        void start(int argc = 0, char *argv[] = NULL);

        virtual void execute(comm::Communicator *comm, int argc = 0, char *argv[] = NULL) = 0;

        void setCommThreads(int comThreads);

        bool isMaster();
    };
}

#endif //HARPC_WORKER_H
