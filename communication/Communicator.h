#ifndef HARPC_COMMUNICATOR_H
#define HARPC_COMMUNICATOR_H

#include <iostream>
#include <memory>
#include "mpi.h"
#include "future"

#include "../util/print.h"
#include "../util/timing.h"
#include "../data_structures/DataStructures.h"
#include "../util/ThreadPool.h"

//todo doing implementation in header file due to templates problem
namespace harp::com {

    class Communicator {
    private:
        int workerId;
        int worldSize;

        //Using separate task queues for each thread pool to prevent unnecessary locking
        //----------------------------------//
        ctpl::thread_pool *threadPool;
        std::queue<std::future<void>> asyncTasks;
        std::mutex asyncTasksMutex;
        //----------------------------------//

        int communicationTag = 0;

    public:
        Communicator(int workerId, int worldSize, int comThreads = 1) {
            this->workerId = workerId;
            this->worldSize = worldSize;
            this->threadPool = new ctpl::thread_pool(comThreads);
        }

        void barrier() {
            MPI_Barrier(MPI_COMM_WORLD);
        }


        /**
         * Table of each worker is distributed to every other worker in the group
         * all Tables are returned as a vector
         * Tables are listed in the vector with respect to their rankings
         * vector[0] has the table from worker 0, vector[1] has the table from worker 1, and so on ...
         *
         * Each partition of the tables are distributed separately, by using MPI allgather.
         * If there are n partitions in worker tables, n mpi-allgather performed to distribute the tables
         * two more all gathers are performed initially to exchage meta data
         *
         * All tables in workers have to have the same number of partitions
         * otherwise, allgather is not performed with this method.
         *
         * @tparam TYPE
         * @param table
         * @return
         */
        template<class TYPE>
        std::vector<ds::Table<TYPE> *> * allGatherAsPartitions(harp::ds::Table<TYPE> *table) {
            // first gather all TableInfo sizes from all workers
            int tableInfoSizes[worldSize];
            ds::TableInfo tableInfo(table);
            int tableInfoSize = tableInfo.getSerializedSize();

            MPI_Allgather(
                    &tableInfoSize,
                    1,
                    MPI_INT,
                    tableInfoSizes,
                    1,
                    MPI_INT,
                    MPI_COMM_WORLD
            );

            // second, gather all TableInfo objects from all workers
            int displacements[worldSize];
            displacements[0] = 0;
            for (int i = 1; i < worldSize; i++) {
                displacements[i] = displacements[i - 1] + tableInfoSizes[i - 1];
            }

            int totalInfoSizes = 0;
            for (int i = 0; i < worldSize; i++) {
                totalInfoSizes += tableInfoSizes[i];
            }

            auto * serializedTableInfo = tableInfo.serialize();
            auto * allInfosSerialized = new int[totalInfoSizes];

            MPI_Allgatherv(
                    serializedTableInfo,
                    tableInfo.getSerializedSize(),
                    MPI_INT,
                    allInfosSerialized,
                    tableInfoSizes,
                    displacements,
                    MPI_INT,
                    MPI_COMM_WORLD
            );

            std::vector<ds::TableInfo *> tableInfos;
            for (int j = 0; j < worldSize; ++j) {
                auto * tInfo = ds::TableInfo::deserialize(allInfosSerialized, displacements[j]);
                tableInfos.push_back(tInfo);
            }

            // if all tables do not have the same number of partitions,
            // do not perform all gather
            int numberOfPartitions = tableInfo.getNumberOfPartitions();
            for (const auto * tInfo: tableInfos) {
                if (numberOfPartitions != tInfo->getNumberOfPartitions()) {
                    std::cout << "To perform AllGather, all tables has to have the same number of partitions." << std::endl;
                    return nullptr;
                }
            }

            // all received tables
            auto * tables = new std::vector<ds::Table<TYPE> *>();
            for (int j = 0; j < worldSize; ++j) {
                tables->push_back(new ds::Table<TYPE>(j));
            }

            // send each partition separately
            int partitionSizes[worldSize];
            for (int k = 0; k < numberOfPartitions; ++k) {

                int totalPartitionSizes = 0;
                int index = 0;
                for (const auto * tInfo: tableInfos) {
                    partitionSizes[index] = tInfo->getPartitionSizes()[k];
                    totalPartitionSizes += partitionSizes[index];
                    index++;
                }

                displacements[0] = 0;
                for (int i = 1; i < worldSize; i++) {
                    displacements[i] = displacements[i - 1] + partitionSizes[i - 1];
                }

                auto * partitionData = table->getPartition(k)->getData();
                auto * allPartitionsData = new int[totalPartitionSizes];
                MPI_Datatype dataType = getMPIDataType<TYPE>();

                MPI_Allgatherv(
                        partitionData,
                        table->getPartition(k)->getSize(),
                        dataType,
                        allPartitionsData,
                        partitionSizes,
                        displacements,
                        dataType,
                        MPI_COMM_WORLD
                );

                // put all received partitions into their corresponding tables
                for (int i = 0; i < worldSize; i++) {
                    TYPE * pdata = new TYPE[partitionSizes[i]];
                    std::copy(allPartitionsData + displacements[i], allPartitionsData + displacements[i] + partitionSizes[i], pdata);
                    auto * partition = new ds::Partition<TYPE>(k, pdata, partitionSizes[i]);
                    (*tables)[i]->addPartition(partition);
                }

                delete [] allPartitionsData;
            }

            // cleanup dynamic objects
            delete [] serializedTableInfo;
            delete [] allInfosSerialized;
            for (auto tInfo: tableInfos) {
                delete tInfo;
            }

            return tables;
        }

        /**
         * Table of each worker is distributed to every worker in the group
         * all Tables are returned as a vector
         * Tables are listed in the vector with respect to their rankings
         * vector[0] has the table from worker 0, vector[1] has the table from worker 1, and so on ...
         *
         * Each table of every worker is first serialized as a single array,
         * then MPI allgather performed,
         * at the last step, all received serialized arrays are converted into Table objects again
         *
         * @tparam TYPE
         * @param table
         * @return
         */
        template<class TYPE>
        std::vector<ds::Table<TYPE> *> * allGatherAsOneArray(harp::ds::Table<TYPE> *table) {
            // first gather all TableInfo sizes from all workers
            int tableInfoSizes[worldSize];
            ds::TableInfo tableInfo(table);
            int tableInfoSize = tableInfo.getSerializedSize();

            MPI_Allgather(
                    &tableInfoSize,
                    1,
                    MPI_INT,
                    tableInfoSizes,
                    1,
                    MPI_INT,
                    MPI_COMM_WORLD
            );

            // second, gather all TableInfo objects from all workers
            int displacements[worldSize];
            displacements[0] = 0;
            for (int i = 1; i < worldSize; i++) {
                displacements[i] = displacements[i - 1] + tableInfoSizes[i - 1];
            }

            int totalInfoSizes = 0;
            for (int i = 0; i < worldSize; i++) {
                totalInfoSizes += tableInfoSizes[i];
            }

            auto * serializedTableInfo = tableInfo.serialize();
            auto * allInfosSerialized = new int[totalInfoSizes];

            MPI_Allgatherv(
                    serializedTableInfo,
                    tableInfo.getSerializedSize(),
                    MPI_INT,
                    allInfosSerialized,
                    tableInfoSizes,
                    displacements,
                    MPI_INT,
                    MPI_COMM_WORLD
            );

            std::vector<ds::TableInfo *> tableInfos;
            for (int j = 0; j < worldSize; ++j) {
                auto * tInfo = ds::TableInfo::deserialize(allInfosSerialized, displacements[j]);
                tableInfos.push_back(tInfo);
            }

            // send Table as a serialized array to all and receive the Tables of all others similarly
            int totalTableSizes = 0;
            int tableSizes[worldSize];
            int index = 0;
            for (const auto * tInfo: tableInfos) {
                tableSizes[index] = tInfo->getSerializedTableSize();
                totalTableSizes += tableSizes[index];
                index++;
            }

            displacements[0] = 0;
            for (int i = 1; i < worldSize; i++) {
                displacements[i] = displacements[i - 1] + tableSizes[i - 1];
            }

            auto * serializedTable = table->serialize();
            auto * allTablesSerialized = new int[totalTableSizes];
            MPI_Datatype dataType = getMPIDataType<TYPE>();

            MPI_Allgatherv(
                    serializedTable,
                    table->getSerializedSize(),
                    dataType,
                    allTablesSerialized,
                    tableSizes,
                    displacements,
                    dataType,
                    MPI_COMM_WORLD
            );

            // all received tables
            auto * tables = std::vector<ds::Table<TYPE> *>();
            for (int j = 0; j < worldSize; ++j) {
                auto * tab = ds::TableInfo::deserializeTable<TYPE>(allTablesSerialized, tableInfos[j], displacements[j]);
                tables->push_back(tab);
            }

            // cleanup dynamic objects
            delete [] serializedTableInfo;
            delete [] allInfosSerialized;
            for (auto tInfo: tableInfos) {
                delete tInfo;
            }
            delete [] serializedTable;
            delete [] allTablesSerialized;

            return tables;
        }


        /**
         * perform allreduce on a table as a single array
         * @tparam TYPE
         * @param table
         * @param operation
         */
        template<class TYPE>
        void allReduceAsOneArray(harp::ds::Table<TYPE> *table, MPI_Op operation) {
            MPI_Datatype dataType = getMPIDataType<TYPE>();
            auto *serializedData = table->serialize();
            auto *reducedData = new TYPE[table->getSerializedSize()];

            MPI_Allreduce(
                    serializedData,
                    reducedData,
                    table->getSerializedSize(),
                    dataType,
                    operation,
                    MPI_COMM_WORLD
            );

            auto *tableInfo = new ds::TableInfo(table);
            auto *reducedTable = ds::TableInfo::deserializeTable(reducedData, tableInfo);
            table->swap(reducedTable);

            delete tableInfo;
        }

        /**
         * perform allreduce each partition separately
         * @tparam TYPE
         * @param table
         * @param operation
         */
        template<class TYPE>
        void allReduceAsPartitions(harp::ds::Table<TYPE> *table, MPI_Op operation) {
            MPI_Datatype dataType = getMPIDataType<TYPE>();
            MPI_Request requests[table->getPartitionCount()];
            std::vector<TYPE *> dataArrays;
            int index = 0;
            for (auto p : *table->getPartitions()) {//keys are ordered in ascending order
                auto *data = new TYPE[p.second->getSize()];
                MPI_Iallreduce(
                        p.second->getData(),
                        data,
                        p.second->getSize(),
                        dataType,
                        operation,
                        MPI_COMM_WORLD,
                        &requests[index++]
                );
                dataArrays.push_back(data);
            }

            MPI_Waitall(static_cast<int>(table->getPartitionCount()), requests, MPI_STATUS_IGNORE);

            index = 0;
            for (auto p : *table->getPartitions()) {
                p.second->setData(dataArrays[index++], p.second->getSize());
            }
        }

        /**
         * broadcast a Table to all others. broadcast each partition separately
         * broadcasting a table happens in three steps:
         *  broadcasting TableInfo size
         *  broadcasting TableInfo
         *  broadcasting each table partition separately
         *
         * @tparam TYPE
         * @param table
         * @param bcastWorkerId
         */
        template<class TYPE>
        void broadcastAsPartitions(harp::ds::Table<TYPE> *table, int bcastWorkerId) {
            // first we broadcast TableInfo size
            int tableInfoSize;
            ds::TableInfo *tableInfo;
            if (bcastWorkerId == this->workerId) {
                tableInfo = new ds::TableInfo(table);
                tableInfoSize = tableInfo->getSerializedSize();
            }

            MPI_Bcast(&tableInfoSize, 1, MPI_INT, bcastWorkerId, MPI_COMM_WORLD);

            // second, we broadcast serialized TableInfo
            int *serializedTableInfo;
            if (bcastWorkerId == this->workerId) {
                serializedTableInfo = tableInfo->serialize();
            } else {
                serializedTableInfo = new int[tableInfoSize];
            }

            MPI_Bcast(serializedTableInfo, tableInfoSize, MPI_INT, bcastWorkerId, MPI_COMM_WORLD);
            if (bcastWorkerId != this->workerId) {
                tableInfo = ds::TableInfo::deserialize(serializedTableInfo);
            }

            //third, we broadcast table partitions
            MPI_Datatype dataType = getMPIDataType<TYPE>();
            int *partitionIds = tableInfo->getPartitionIDs();
            int *partitionSizes = tableInfo->getPartitionSizes();

            for (long i = 0; i < tableInfo->getNumberOfPartitions(); i++) {
                if (partitionSizes[i] > 0) {
                    TYPE *data;
                    if (bcastWorkerId == this->workerId) {
                        data = table->getPartition(partitionIds[i])->getData();
                    } else {
                        data = new TYPE[partitionSizes[i]];
                    }
                    MPI_Bcast(data, partitionSizes[i], dataType, bcastWorkerId, MPI_COMM_WORLD);
                    if (bcastWorkerId != this->workerId) {
                        auto *newPartition = new harp::ds::Partition<TYPE>(partitionIds[i], data, partitionSizes[i]);
                        table->addPartition(newPartition);
                    }
                }
            }

            // delete intermediate data
            delete tableInfo;
            delete[] serializedTableInfo;
        }

        /**
         * broadcast a Table to all others. broadcast all partitions as a single array
         * broadcasting a table happens in three steps:
         *  broadcasting TableInfo size
         *  broadcasting TableInfo
         *  broadcasting all partitions as a single array
         *
         * @tparam TYPE
         * @param table
         * @param bcastWorkerId
         */
        template<class TYPE>
        void broadcastAsOneArray(harp::ds::Table<TYPE> *table, int bcastWorkerId) {
            // first we broadcast TableInfo size
            int tableInfoSize;
            ds::TableInfo *tableInfo;
            if (bcastWorkerId == this->workerId) {
                tableInfo = new ds::TableInfo(table);
                tableInfoSize = tableInfo->getSerializedSize();
            }

            MPI_Bcast(&tableInfoSize, 1, MPI_INT, bcastWorkerId, MPI_COMM_WORLD);

            // second, we broadcast serialized TableInfo
            int *serializedTableInfo;
            if (bcastWorkerId == this->workerId) {
                serializedTableInfo = tableInfo->serialize();
            } else {
                serializedTableInfo = new int[tableInfoSize];
            }

            MPI_Bcast(serializedTableInfo, tableInfoSize, MPI_INT, bcastWorkerId, MPI_COMM_WORLD);
            if (bcastWorkerId != this->workerId) {
                tableInfo = ds::TableInfo::deserialize(serializedTableInfo);
            }

            //third, we broadcast table as an array
            MPI_Datatype dataType = getMPIDataType<TYPE>();

            TYPE *serializedTableData;
            if (bcastWorkerId == this->workerId) {
                serializedTableData = table->serialize();
            } else {
                serializedTableData = new TYPE[tableInfo->getSerializedTableSize()];
            }
            MPI_Bcast(serializedTableData, tableInfo->getSerializedTableSize(), dataType, bcastWorkerId,
                      MPI_COMM_WORLD);
            if (bcastWorkerId != this->workerId) {
                auto *newTable = ds::TableInfo::deserializeTable(serializedTableData, tableInfo);
                table->swap(newTable);
            }

            // delete intermediate data
            delete tableInfo;
            delete[] serializedTableInfo;
            delete[] serializedTableData;
        }

        template<class TYPE>
        void rotate(harp::ds::Table<TYPE> *table, int sendTag = -1, int recvTag = -1) {
            MPI_Datatype dataType = getMPIDataType<TYPE>();

            int sendTo = (this->workerId + 1) % this->worldSize;
            int receiveFrom = (this->workerId + this->worldSize - 1) % this->worldSize;

            //exchange NUMBER OF PARTITIONS
            int numOfPartitionsToSend = static_cast<int>(table->getPartitionCount());
            int numOfPartitionsToRecv = 0;


            //std::cout << "Will send " << numOfPartitionsToSend << " partitions to " << sendTo << std::endl;

            MPI_Sendrecv(
                    &numOfPartitionsToSend, 1, MPI_INT, sendTo, sendTag,
                    &numOfPartitionsToRecv, 1, MPI_INT, receiveFrom, recvTag,
                    MPI_COMM_WORLD,
                    MPI_STATUS_IGNORE
            );

            //std::cout << "Will recv " << numOfPartitionsToRecv << " from " << receiveFrom << std::endl;

            //exchange PARTITION METADATA
            int sendingMetaSize = 1 + (numOfPartitionsToSend * 2);// totalDataSize(1) + [{id, size}]
            int receivingMetaSize = 1 + (numOfPartitionsToRecv * 2);// totalDataSize(1) + [{id, size}]

            int partitionMetaToSend[sendingMetaSize];
            int partitionMetaToRecv[receivingMetaSize];

            int index = 1;
            int totalDataSize = 0;
            std::vector<TYPE> dataBuffer;//todo possible error: data buffer gets cleared immediately after returning this function

            for (const auto p : *table->getPartitions()) {
                partitionMetaToSend[index++] = p.first;
                partitionMetaToSend[index++] = p.second->getSize();
                totalDataSize += p.second->getSize();
                //todo prevent memory copying if possible
                std::copy(p.second->getData(), p.second->getData() + p.second->getSize(),
                          std::back_inserter(dataBuffer));
            }
            partitionMetaToSend[0] = totalDataSize;

            //std::cout << "Will send " << partitionMetaToSend[0] << " elements to " << sendTo << std::endl;

            MPI_Sendrecv(
                    &partitionMetaToSend, sendingMetaSize, MPI_INT, sendTo, sendTag,
                    &partitionMetaToRecv, receivingMetaSize, MPI_INT, receiveFrom, recvTag,
                    MPI_COMM_WORLD,
                    MPI_STATUS_IGNORE
            );

            //std::cout << "Will recv " << partitionMetaToRecv[0] << " from " << receiveFrom << std::endl;

            //sending DATA
            //todo implement support for data arrays larger than INT_MAX
            MPI_Request dataSendRequest;
            MPI_Isend(&dataBuffer[0], totalDataSize, dataType, sendTo, sendTag, MPI_COMM_WORLD,
                      &dataSendRequest);

            auto *recvTab = new harp::ds::Table<TYPE>(table->getId());
            auto *recvBuffer = new TYPE[partitionMetaToRecv[0]];

            MPI_Recv(recvBuffer, partitionMetaToRecv[0], dataType, receiveFrom, recvTag,
                     MPI_COMM_WORLD,
                     MPI_STATUS_IGNORE);

            int copiedCount = 0;
            for (long i = 1; i < receivingMetaSize; i += 2) {
                int partitionId = partitionMetaToRecv[i];
                int partitionSize = partitionMetaToRecv[i + 1];

                auto *data = new TYPE[partitionSize];
                std::copy(recvBuffer + copiedCount, recvBuffer + copiedCount + partitionSize, data);
                copiedCount += partitionSize;

                auto *newPartition = new harp::ds::Partition<TYPE>(partitionId, data, partitionSize);
                recvTab->addPartition(newPartition);
            }

            MPI_Wait(&dataSendRequest, MPI_STATUS_IGNORE);

            //delete table;
            table->swap(recvTab);
            harp::ds::util::deleteTable(recvTab, false);

        }

        template<class TYPE>
        void asyncRotate(harp::ds::Table<TYPE> *table, int pid) {
            auto partition = table->getPartition(pid);//take partition out
            table->removePartition(pid, false);//this happens in the same thread all the time
            auto *rotatingTable = new harp::ds::Table<TYPE>(table->getId());//create new table for rotation
            rotatingTable->addPartition(partition);

            int myReceiveTag = table->getId();
            int mySendTag = table->getId();

            if (this->threadPool->size() > 1) {// for just one thread, communication happens one after another.
                int sendTo = (this->workerId + 1) % this->worldSize;
                int receiveFrom = (this->workerId + this->worldSize - 1) % this->worldSize;

                myReceiveTag = this->communicationTag++;
                mySendTag = -1;

                MPI_Sendrecv(
                        &myReceiveTag, 1, MPI_INT, receiveFrom, table->getId(),
                        &mySendTag, 1, MPI_INT, sendTo, table->getId(),
                        MPI_COMM_WORLD,
                        MPI_STATUS_IGNORE
                );
            }

            std::future<void> rotateTaskFuture = this->threadPool->push(
                    [rotatingTable, table, mySendTag, myReceiveTag, this](int id) {
                        //std::cout << "Executing rotate in thread : " << id << std::endl;
                        rotate(rotatingTable, mySendTag, myReceiveTag);
                        if (this->threadPool->size() > 1) {
                            //for a single thread, there is no need in locking
                            this->asyncTasksMutex.lock();
                            for (auto p:*rotatingTable->getPartitions()) {
                                table->addToPendingPartitions(p.second);
                            }
                            this->asyncTasksMutex.unlock();
                        } else {
                            for (auto p:*rotatingTable->getPartitions()) {
                                table->addToPendingPartitions(p.second);
                            }
                        }
                    });

            //assuming this happens always on the same thread
            this->asyncTasks.push(std::move(rotateTaskFuture));
        }

        int getWorkerId() {
            return this->workerId;
        }

        int getWorldSize() {
            return this->worldSize;
        }

        void wait() {
            //wait for async task thread pool queue
            while (!this->asyncTasks.empty()) {
                this->asyncTasks.front().get();
                this->asyncTasks.pop();
            }
        }
    };

} // end of namespace harp::com
#endif //HARPC_COMMUNICATOR_H
