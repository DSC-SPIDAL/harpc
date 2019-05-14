//
// Created by auyar on 13.05.2019.
//

#include "TableInfo.h"
#include "Table.h"

namespace harp::ds {

    TableInfo::TableInfo(int id, int nOfPartitions, int * ids, int * sizes) :
    tableID(id), numberOfPartitions(nOfPartitions) {
        partitionIDs = ids;
        partitionSizes = sizes;
    }

    TableInfo::~TableInfo() {
        delete[] partitionIDs;
        delete[] partitionSizes;
    }

    int TableInfo::getTableID() const {
        return tableID;
    }

    int TableInfo::getNumberOfPartitions() const {
        return numberOfPartitions;
    }

    int TableInfo::getPartitionSize(int pid) const {
        // first find pid in id array
        for (int i = 0; i < numberOfPartitions; ++i) {
            if (pid == partitionIDs[i]) {
                return partitionSizes[i];
            }
        }

        return 0;
    }

    int * TableInfo::getPartitionIDs() const {
        return partitionIDs;
    }

    int * TableInfo::getPartitionSizes() const {
        return partitionSizes;
    }

    int TableInfo::getSerializedTableSize() const {
        int size = 0;
        for (int i = 0; i < numberOfPartitions; ++i) {
            size += partitionSizes[i];
        }
        return size;
    }

    int * TableInfo::serialize() {

        int * data = new int[getSerializedSize()];
        data[0] = tableID;
        data[1] = numberOfPartitions;
        int index = 2;
        for (int i = 0; i < numberOfPartitions; ++i) {
            data[index++] = partitionIDs[i];
            data[index++] = partitionSizes[i];
        }

        return data;
    }

    TableInfo * TableInfo::deserialize(int * data, int offset) {

        int tId = data[offset];
        int nOfPartitions = data[offset + 1];
        int * ids = new int[nOfPartitions];
        int * sizes = new int[nOfPartitions];
        int index = offset + 2;
        for (int i = 0; i < nOfPartitions; ++i) {
            ids[i] = data[index++];
            sizes[i] = data[index++];
        }

        return new TableInfo(tId, nOfPartitions, ids, sizes);
    }

    int TableInfo::getSerializedSize() {
        return 2 + numberOfPartitions + numberOfPartitions;
    }

    void TableInfo::print() const {
        std::cout << "------------------------------------" << std::endl;

        std::cout << "id: " << tableID << ", partitions: " << numberOfPartitions << std::endl;
        for (int j = 0; j < numberOfPartitions; j++) {
            std::cout << "[" << partitionIDs[j] << ", " << partitionSizes[j] << "] ";
        }
        std::cout << std::endl;
        std::cout << "------------------------------------" << std::endl;
    }


} // end of namespace harp::ds