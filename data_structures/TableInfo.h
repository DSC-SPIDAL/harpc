//
// Created by auyar on 08.05.2019.
//

#ifndef HARPC_TABLEINFO_H
#define HARPC_TABLEINFO_H

#include "Table.h"

namespace harp::ds {

    class TableInfo {
    private:
        long numberOfPartitions;
        int * partitionIDs;
        int * partitionSizes;

    public:
        template<class TYPE>
        TableInfo(Table<TYPE> * table) : numberOfPartitions(table->getPartitionCount()) {

            partitionIDs = new int[numberOfPartitions];
            partitionSizes = new int[numberOfPartitions];

            int index = 0;
            for (const auto p : *table->getPartitions()) {
                partitionIDs[index] = p.first;
                partitionSizes[index] = p.second->getSize();
                index++;
            }
        }

        TableInfo(int nOfPartitions, int * ids, int * sizes) : numberOfPartitions(nOfPartitions) {
            partitionIDs = ids;
            partitionSizes = sizes;
        }

        ~TableInfo() {
            delete[] partitionIDs;
            delete[] partitionSizes;
        }

        int getNumberOfPartitions() {
            return numberOfPartitions;
        }

        int * getPartitionIDs() {
            return partitionIDs;
        }

        int * getPartitionSizes() {
            return partitionSizes;
        }

        int * serialize() {

            int * data = new int[getSerializedSize()];
            data[0] = numberOfPartitions;
            int index = 1;
            for (int i = 0; i < numberOfPartitions; ++i) {
                data[index++] = partitionIDs[i];
                data[index++] = partitionSizes[i];
            }

            return data;
        }

        static TableInfo * deserialize(int * data) {

            int nOfPartitions = data[0];
            int * ids = new int[nOfPartitions];
            int * sizes = new int[nOfPartitions];
            int index = 1;
            for (int i = 0; i < nOfPartitions; ++i) {
                ids[i] = data[index++];
                sizes[i] = data[index++];
            }

            return new TableInfo(nOfPartitions, ids, sizes);
        }

        int getSerializedSize() {
            return 1 + numberOfPartitions + numberOfPartitions;
        }
    };
} // end of namespace
#endif //HARPC_TABLEINFO_H
