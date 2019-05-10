//
// Created by auyar on 08.05.2019.
//

#ifndef HARPC_TABLEINFO_H
#define HARPC_TABLEINFO_H

#include <algorithm>
#include "Table.h"

namespace harp::ds {

    class TableInfo {
    private:
        int tableID;
        long numberOfPartitions;
        int * partitionIDs;
        int * partitionSizes;

    public:
        template<class TYPE>
        TableInfo(Table<TYPE> * table) : numberOfPartitions(table->getPartitionCount()), tableID(table->getId()) {

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

        int getTableID() {
            return tableID;
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

        /**
         * total of all partition sizes
         * this is equal to serialized Table data size
         * @return
         */
        int getSerializedTableSize() {
            int size = 0;
            for (int i = 0; i < numberOfPartitions; ++i) {
                size += partitionSizes[i];
            }
            return size;
        }

        int * serialize() {

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

        /**
         * deserialize TableInfo
         * @param data
         * @return
         */
        static TableInfo * deserialize(int * data) {

            int tId = data[0];
            int nOfPartitions = data[1];
            int * ids = new int[nOfPartitions];
            int * sizes = new int[nOfPartitions];
            int index = 2;
            for (int i = 0; i < nOfPartitions; ++i) {
                ids[i] = data[index++];
                sizes[i] = data[index++];
            }

            return new TableInfo(nOfPartitions, ids, sizes);
        }

        int getSerializedSize() {
            return 2 + numberOfPartitions + numberOfPartitions;
        }

        template<class TYPE>
        static Table<TYPE> * deserializeTable(TYPE * serializedData, TableInfo * tableInfo) {
            Table<TYPE> * table = new Table<TYPE>(tableInfo->getTableID());

            int dataIndex = 0;

            for (int i = 0; i < tableInfo->getNumberOfPartitions(); ++i) {
                int pSize = tableInfo->getPartitionSizes()[i];
                int pID = tableInfo->getPartitionIDs()[i];
                TYPE * pData = new TYPE[pSize];
                std::copy(serializedData + dataIndex, serializedData + dataIndex + pSize, pData);
                dataIndex += pSize;

                Partition<TYPE> * partition = new Partition<TYPE>(pID, pData, pSize);
                table->addPartition(partition);
            }

            return table;
        }

    };
} // end of namespace
#endif //HARPC_TABLEINFO_H
