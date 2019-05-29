#include <iostream>
#include <map>

#include "harp.h"
#include <time.h>
#include <TableInfo.h>

#include "DataTypes.h"

using namespace std;
using namespace harp::ds;
using namespace harp::comm;

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

/**
 * create a Table with given properties
 * all properties shows max value
 * all properties are assigned random values
 * @param tableId
 * @param maxPartitions
 * @param maxPartitionSize
 * @param maxValue
 * @return
 */
template<class TYPE>
Table<TYPE> * createTable(int tableId, int maxPartitions, int maxPartitionSize, int maxValue, int workerId=0) {
    auto *tab = new Table<TYPE>(tableId);

    srand(workerId + time(NULL));
//    int numOfPartitions = 3;
    int numOfPartitions = rand() % maxPartitions;
//    cout << "number of partitions: " << numOfPartitions << endl;

    for (int p = 0; p < numOfPartitions; p++) {
        int partitionSize = rand() % maxPartitionSize + 1;
        TYPE *data = new TYPE[partitionSize];
        for (int j = 0; j < partitionSize; j++) {
            data[j] = rand() % maxValue;
        }
        auto *partition = new Partition<TYPE>(p, data, partitionSize);
        tab->addPartition(partition);
    }

    return tab;
}

class MyWorker : public harp::worker::Worker {

    void execute(Communicator *comm, int argc = 0, char *argv[] = NULL) override {

//        testBroadcast(comm);
//        testAllReduce(comm);
//        testGather(comm);
        testAllGather(comm);
    }

    void testBroadcast(Communicator *comm) {

        Table<int> * tab;

        // populate the table for the first worker
        // this table will be broadcasted to all
        if (this->workerId == 0) {
            tab = createTable<int>(0, 5, 20, 1000);
            cout <<  "original table: " << endl;
            printTable(workerId, tab);

            // tables of other workers will be empty
        } else {
            tab = new Table<int>(0);
        }

        // broadcast from 0 to all
//        comm->broadcastAsOneArray(tab, 0);
        comm->broadcastAsPartitions(tab, 0);

        printTable(workerId, tab);

        tab->clear();
    }

    void testAllReduce(Communicator *comm) {

        Table<int> * tab;
        tab = createTable<int>(0, 3, 10, 1000);
        printTable(workerId, tab);

        // broadcast from 0 to all
//        comm->allReduceAsPartitions(tab, MPI_SUM);
        comm->allReduceAsOneArray(tab, MPI_SUM);

        cout << "table after allReduce" << endl;
        printTable(workerId, tab);

        tab->clear();
    }

    void testGather(Communicator *comm) {

        Table<double> * tab = createTable<double>(0, 4, 10, 1000, workerId);
        printTable(workerId, tab);

        int rootWorkerId = 3;
//        auto tables = comm->gatherAsPartitions(tab, rootWorkerId);
        auto tables = comm->gatherAsOneArray(tab, rootWorkerId);

        if (workerId == rootWorkerId) {
            cout << "tables after gather at worker: " << workerId  << endl;
            for (auto * table : *tables) {
                printTable(table->getId(), table);
//                harp::utils::print::printTable(table);
            }
        }

        tab->clear();
    }

    void testAllGather(Communicator *comm) {

        Table<double> * tab = createTable<double>(0, 4, 10, 1000, workerId);
        printTable(workerId, tab);

        // broadcast from 0 to all
        auto tables = comm->allGatherAsOneArray(tab);
//        auto tables = comm->allGatherAsPartitions(tab);

        if (workerId == 1) {
            cout << "tables after all gather at worker: " << workerId << endl;
            for (auto * table : *tables) {
                printTable(table->getId(), table);
//                harp::utils::print::printTable(table);
            }
        }

        cout << "table after allGather" << endl;
//        printTable(workerId, tab);

        tab->clear();
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


}; // end of MyWorker



int main() {

    MyWorker worker;
    worker.init(0, nullptr);
    worker.start();
    return 0;
}




