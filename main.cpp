#include <iostream>
#include <map>

#include "harp.h"
#include <time.h>

#include "DataTypes.h"

using namespace std;
using namespace harp::ds;
using namespace harp::com;

template<class TYPE>
void printTable(int worker, Table<TYPE> *tab) {
    string pri = "\n" + to_string(worker) + " [";
    for (const auto f: *tab->getPartitions()) {
        auto data = f.second->getData();
        for (int i = 0; i< f.second->getSize(); i++) {
            pri += to_string(data[i]) + ",";
        }
        pri += "|";
    }
    pri += "]";
    cout << pri << endl;
}

class MyWorker : public harp::worker::Worker {

    void execute(Communicator *comm, int argc = 0, char *argv[] = NULL) override {

        testBroadcast(comm);
    }

    void testRotate(Communicator *comm) {
        auto *tab = new Table<int>(1);

        srand(workerId + time(NULL));
        int numOfPartitions = rand() % 40;
        for (int p = 0; p < numOfPartitions; p++) {
            int partitionSize = rand() % 1000000;
            int *data = new int[partitionSize];
            for (int j = 0; j < partitionSize; j++) {
                data[j] = rand() % 100;
            }
            auto *partition = new Partition<int>(p, data, partitionSize);
            tab->addPartition(partition);
        }

        //printPartitions(workerId, tab);
        cout << this->workerId << " arrived at the barrier" << endl;
        comm->barrier();
        cout << this->workerId << " released from the barrier" << endl;


        comm->rotate(tab);
//        comm->barrier();
//        printPartitions(workerId, &tab);
//        comm->broadcast(tab, 0);
        //comm->allReduce(&tab, MPI_SUM);

        cout << this->workerId << " bcast complete" << endl;
        comm->barrier();
        //printPartitions(workerId, tab);
        tab->clear();
    }

    void testBroadcast(Communicator *comm) {

        auto *tab = new Table<double>(1);

        // populate the table for the first worker
        // this table will be broadcasted to all
        if (this->workerId == 0) {
            srand(workerId + time(NULL));
            int numOfPartitions = rand() % 40;
            for (int p = 0; p < numOfPartitions; p++) {
                int partitionSize = rand() % 100;
                auto *data = new double[partitionSize];
                for (int j = 0; j < partitionSize; j++) {
                    data[j] = rand() % 100;
                }
                auto *partition = new Partition<double>(p, data, partitionSize);
                tab->addPartition(partition);
            }


            cout <<  "original table: " << endl;
            printTable(workerId, tab);
        }

        // broadcast from 0 to all
        comm->broadcast(tab, 0);

        printTable(workerId, tab);

        tab->clear();
    }

};


int main() {

    cout << "starting ..." << endl;
    MyWorker worker;
    worker.init(0, nullptr);
    worker.start();
    return 0;
}




