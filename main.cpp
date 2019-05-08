#include <iostream>
#include <map>

#include "harp.h"
#include <time.h>
#include <TableInfo.h>

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
Table<int> * createTableInt(int tableId, int maxPartitions, int maxPartitionSize, int maxValue) {
    auto *tab = new Table<int>(tableId);

    srand(time(NULL));
    int numOfPartitions = rand() % maxPartitions;
    cout << "number of partitions: " << numOfPartitions << endl;

    for (int p = 0; p < numOfPartitions; p++) {
        int partitionSize = rand() % maxPartitionSize;
        int *data = new int[partitionSize];
        for (int j = 0; j < partitionSize; j++) {
            data[j] = rand() % maxValue;
        }
        auto *partition = new Partition<int>(p, data, partitionSize);
        tab->addPartition(partition);
    }

    return tab;
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

        Table<int> * tab;

        // populate the table for the first worker
        // this table will be broadcasted to all
        if (this->workerId == 0) {
            tab = createTableInt(0, 10, 100, 1000);
            cout <<  "original table: " << endl;
            printTable(workerId, tab);

        // tables of other workers will be empty
        } else {
            tab = new Table<int>(0);
        }

        // broadcast from 0 to all
        comm->broadcast(tab, 0);

        printTable(workerId, tab);

        tab->clear();
    }

};


void testTableInfo() {

    auto * table = createTableInt(0, 10, 100, 1000);
    cout <<  "original table: " << endl;
    printTable(0, table);

    TableInfo * tableInfo = new TableInfo(table);

    cout << "number of partitions: " << tableInfo->getNumberOfPartitions() << endl;
    int * ids = tableInfo->getPartitionIDs();
    int * sizes = tableInfo->getPartitionSizes();
    for (int i = 0; i < tableInfo->getNumberOfPartitions(); ++i) {
        cout << ids[i] << ": " << sizes[i] << ", " << endl;
    }

    int * sdata = tableInfo->serialize();
    cout << "serialized tableinfo: " << endl;
    for (int j = 0; j < tableInfo->getSerializedSize(); ++j) {
        cout << sdata[j] << ", ";
    }

    TableInfo * tableInfo2 = TableInfo::deserialize(sdata);
    cout << "number of partitions: " << tableInfo2->getNumberOfPartitions() << endl;
    ids = tableInfo2->getPartitionIDs();
    sizes = tableInfo2->getPartitionSizes();
    for (int i = 0; i < tableInfo2->getNumberOfPartitions(); ++i) {
        cout << ids[i] << ": " << sizes[i] << ", " << endl;
    }
}

int main() {

    cout << "starting ..." << endl;
//    testTableInfo();

    MyWorker worker;
    worker.init(0, nullptr);
    worker.start();
    return 0;
}




