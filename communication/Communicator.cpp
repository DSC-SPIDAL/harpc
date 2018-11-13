#include "Communicator.h"
#include "mpi.h"
#include <map>

namespace harp {
    namespace com {

        void sendAndRecv(const void *buffSend, int sendSize, void *buffRecv, int recvSize, int sendTo, int recvFrom,
                         MPI_Datatype mpiDatatype) {
            MPI_Request mpi_request;
            MPI_Isend(buffSend, sendSize, mpiDatatype, sendTo, 0, MPI_COMM_WORLD, &mpi_request);
            MPI_Recv(buffRecv, recvSize, mpiDatatype, recvFrom, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Wait(&mpi_request, MPI_STATUSES_IGNORE);
        }

        Communicator::Communicator(int workerId, int worldSize) {
            this->workerId = workerId;
            this->worldSize = worldSize;
        }

        template<class TYPE>
        void Communicator::allGather(harp::ds::Table<TYPE> *table) {

        }

        template<class TYPE>
        void Communicator::allReduce(harp::ds::Table<TYPE> *table, MPI_Op operation) {
            MPI_Datatype dataType = getMPIDataType(table->getDataType());
            for (auto p : table->getPartitions()) {//keys are ordered
                auto *data = createArray(table->getDataType(), p.second->getSize());
                MPI_Allreduce(
                        p.second->getData(),
                        data,
                        p.second->getSize(),
                        dataType,
                        operation,
                        MPI_COMM_WORLD
                );
                p.second->setData(data);
            }
        }

        template<class TYPE>
        void Communicator::broadcast(harp::ds::Table<TYPE> *table, int bcastWorkerId) {
            //determining number of partitions to bcast
            int partitionCount;
            if (bcastWorkerId == this->workerId) {
                partitionCount = static_cast<int>(table->getPartitionCount());
            }
            MPI_Bcast(&partitionCount, 1, MPI_INT, bcastWorkerId, MPI_COMM_WORLD);

            //broadcasting partition ids and sizes
            int partitionIds[partitionCount * 2];// [id, size]
            int index = 0;
            if (bcastWorkerId == this->workerId) {
                for (const auto p : table->getPartitions()) {
                    partitionIds[index++] = p.first;
                    partitionIds[index++] = p.second->getSize();
                }
            }
            MPI_Bcast(&partitionIds, partitionCount * 2, MPI_INT, bcastWorkerId, MPI_COMM_WORLD);

            MPI_Datatype dataType = getMPIDataType(table->getDataType());

            //now receiving partitions
            for (long i = 0; i < partitionCount * 2; i += 2) {
                int partitionId = partitionIds[i];
                int partitionSize = partitionIds[i + 1];
                if (partitionSize > 0) {
                    auto *data = createArray(table->getDataType(),
                                             partitionSize);
                    if (bcastWorkerId == this->workerId) {
                        data = table->getPartition(partitionId)->getData();
                    }
                    MPI_Bcast(data, partitionSize, dataType, bcastWorkerId, MPI_COMM_WORLD);
                    if (bcastWorkerId != this->workerId) {
                        auto *newPartition = new harp::ds::Partition<TYPE>(partitionId, data,
                                                                           partitionSize,
                                                                           table->getDataType());
                        table->addPartition(newPartition);
                    }
                }
            }
        }

        template<class TYPE>
        void Communicator::rotate(harp::ds::Table<TYPE> *table) {
            MPI_Datatype dataType = getMPIDataType(table->getDataType());

            int sendTo = (this->workerId + 1) % this->worldSize;
            int receiveFrom = (this->workerId + this->worldSize - 1) % this->worldSize;

            //exchange NUMBER OF PARTITIONS
            int numOfPartitionsToSend = static_cast<int>(table->getPartitionCount());
            int numOfPartitionsToRecv = 0;
            sendAndRecv(&numOfPartitionsToSend, 1, &numOfPartitionsToRecv, 1, sendTo, receiveFrom, MPI_INT);

            printf("Worker %d will send %d partitions and receive %d partitions\n", workerId, numOfPartitionsToSend,
                   numOfPartitionsToRecv);

            //exchange PARTITION SIZES
            int partitionIdsToSend[numOfPartitionsToSend * 2];// [id, size]
            int partitionIdsToRecv[numOfPartitionsToRecv * 2];// [id, size]
            int index = 0;
            for (const auto p : table->getPartitions()) {
                partitionIdsToSend[index++] = p.first;
                partitionIdsToSend[index++] = p.second->getSize();
            }
            sendAndRecv(&partitionIdsToSend, numOfPartitionsToSend * 2,
                        &partitionIdsToRecv, numOfPartitionsToRecv * 2,
                        sendTo,
                        receiveFrom,
                        MPI_INT);

            //sending DATA
            MPI_Request dataSendRequests[numOfPartitionsToSend];
            for (long i = 0; i < numOfPartitionsToSend * 2; i += 2) {
                int partitionId = partitionIdsToSend[i];
                int partitionSize = partitionIdsToSend[i + 1];
                auto *data = table->getPartition(partitionId)->getData();
                MPI_Isend(data, partitionSize, dataType, sendTo, partitionId, MPI_COMM_WORLD,
                          &dataSendRequests[i / 2]);
            }

            //table->clear();

            auto *recvTab = new harp::ds::Table<TYPE>(table->getId(), table->getDataType());

            //receiving DATA
            for (long i = 0; i < numOfPartitionsToRecv * 2; i += 2) {
                int partitionId = partitionIdsToRecv[i];
                int partitionSize = partitionIdsToRecv[i + 1];
                auto *data = createArray(table->getDataType(),
                                         partitionSize);
                MPI_Recv(data, partitionSize, dataType, receiveFrom, partitionId, MPI_COMM_WORLD,
                         MPI_STATUS_IGNORE);
                auto *newPartition = new harp::ds::Partition<TYPE>(partitionId, data, partitionSize,
                                                                   table->getDataType());
                recvTab->addPartition(newPartition);
            }

            MPI_Waitall(numOfPartitionsToSend, dataSendRequests, MPI_STATUS_IGNORE);
            delete table;
            *table = *recvTab;
        }

        void Communicator::barrier() {
            MPI_Barrier(MPI_COMM_WORLD);
        }
    }
}