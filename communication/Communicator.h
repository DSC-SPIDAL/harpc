#ifndef HARPC_COMMUNICATOR_H
#define HARPC_COMMUNICATOR_H

#include <iostream>
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


        template<class TYPE>
        void allGather(harp::ds::Table<TYPE> *table) {
            auto *partitionCounts = new int[worldSize];//no of partitions in each node
            int partitionCount = static_cast<int>(table->getPartitionCount());
            MPI_Allgather(
                    &partitionCount,
                    1,
                    MPI_INT,
                    partitionCounts,
                    1,
                    MPI_INT,
                    MPI_COMM_WORLD
            );


            int totalPartitionsInWorld = 0;
            int *recvCounts = new int[worldSize];
            int *displacements = new int[worldSize];
            displacements[0] = 0;
            for (int i = 0; i < worldSize; i++) {
                totalPartitionsInWorld += partitionCounts[i];
                recvCounts[i] = 1 + (partitionCounts[i] * 2);//size + [{id,size}]
                if (i != 0) {
                    displacements[i] = displacements[i - 1] + recvCounts[i];
                }
            }

            int worldPartitionMetaDataSize = worldSize +
                                             (totalPartitionsInWorld *
                                              2);  //1*worldSize(for sizes) + [{id,size}]
            long *worldPartitionMetaData = new long[worldPartitionMetaDataSize];

            long thisNodeTotalDataSize = 0;//total size of data

            int thisNodeMetaSendSize = static_cast<int>(1 +
                                                        (table->getPartitionCount() * 2));//totalSize + [{id,size}]
            long *thisNodePartitionMetaData = new long[thisNodeMetaSendSize];

            int pIdIndex = 1;

            std::vector<TYPE> thisNodeDataBuffer;
            for (const auto p : *table->getPartitions()) {
                std::copy(p.second->getData(), p.second->getData() + p.second->getSize(),
                          std::back_inserter(thisNodeDataBuffer));
                thisNodeTotalDataSize += p.second->getSize();
                thisNodePartitionMetaData[pIdIndex++] = p.first;
                thisNodePartitionMetaData[pIdIndex++] = p.second->getSize();
            }
            thisNodePartitionMetaData[0] = thisNodeTotalDataSize;

//                std::cout << "Node " << workerId << " total size : " << thisNodeTotalDataSize << std::endl;
//
//                harp::util::print::printArray(thisNodePartitionMetaData, thisNodeMetaSendSize);


            MPI_Allgatherv(
                    thisNodePartitionMetaData,
                    thisNodeMetaSendSize,
                    MPI_LONG,
                    worldPartitionMetaData,
                    recvCounts,
                    displacements,
                    MPI_LONG,
                    MPI_COMM_WORLD
            );

            // done meta data exchange
//                if (workerId == 0) {
//                    harp::util::print::printArray(worldPartitionMetaData, worldPartitionMetaDataSize);
//                }

            long totalDataSize = worldPartitionMetaData[0];
            int tempLastIndex = 0;
            recvCounts[0] = static_cast<int>(worldPartitionMetaData[0]); //todo cast?
            displacements[0] = 0;
            for (int i = 1; i < worldSize; i++) {
                int thisIndex = tempLastIndex + (partitionCounts[i - 1] * 2) + 1;
                displacements[i] = static_cast<int>(totalDataSize);
                totalDataSize += worldPartitionMetaData[thisIndex];
                recvCounts[i] = static_cast<int>(worldPartitionMetaData[thisIndex]);
                tempLastIndex = thisIndex;
            }

            MPI_Datatype dataType = getMPIDataType<TYPE>();

            auto *worldDataBuffer = new TYPE[totalDataSize];
            MPI_Allgatherv(
                    &thisNodeDataBuffer[0],
                    static_cast<int>(thisNodeTotalDataSize),
                    dataType,
                    worldDataBuffer,
                    recvCounts,
                    displacements,
                    dataType,
                    MPI_COMM_WORLD
            );

//                if (workerId == 0) {
//                    harp::util::print::printArray(worldDataBuffer, totalDataSize);
//                }

            //now we have all data
            auto *recvTab = new harp::ds::Table<TYPE>(table->getId());
            int lastIndex = 0;
            int metaIndex = 0;
            for (int i = 0; i < worldSize; i++) {
                int noOfPartitions = partitionCounts[i];
                metaIndex++;
                for (int j = 0; j < noOfPartitions; j++) {
                    int partitionId = static_cast<int>(worldPartitionMetaData[metaIndex++]);
                    int partitionSize = static_cast<int>(worldPartitionMetaData[metaIndex++]);
                    auto *partitionData = new TYPE[partitionSize];
                    auto *partition = new harp::ds::Partition<TYPE>(partitionId, partitionData, partitionSize);
                    std::copy(worldDataBuffer + lastIndex, worldDataBuffer + lastIndex + partitionSize,
                              partitionData);
//                        if (workerId == 0) {
//                            std::cout << "pSize:" << partitionSize << std::endl;
//                            harp::util::print::printPartition(partition);
//                        }
                    lastIndex += partitionSize;
                    recvTab->addPartition(partition);
                }
            }

            table->swap(recvTab);
            harp::ds::util::deleteTable(recvTab, false);
        }

        template<class TYPE>
        void allReduce(harp::ds::Table<TYPE> *table, MPI_Op operation) {
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
         *  todo: broadcasting whole table at once, instead of sending each partition separately
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

            TYPE * serializedTableData;
            if (bcastWorkerId == this->workerId) {
                serializedTableData = table->serialize();
            } else {
                serializedTableData = new TYPE[tableInfo->getSerializedTableSize()];
            }
            MPI_Bcast(serializedTableData, tableInfo->getSerializedTableSize(), dataType, bcastWorkerId, MPI_COMM_WORLD);
            if (bcastWorkerId != this->workerId) {
                auto * newTable = ds::TableInfo::deserializeTable(serializedTableData, tableInfo);
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
