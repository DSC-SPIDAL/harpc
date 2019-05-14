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

        TableInfo(int id, int nOfPartitions, int * ids, int * sizes);

        ~TableInfo();

        int getTableID() const ;

        int getNumberOfPartitions() const ;

        int * getPartitionIDs() const ;

        int * getPartitionSizes() const ;

        /**
         * return partition size for the given partition id
         * zero returned if the partition does not exist or partition size is zero
         * @param pid
         * @return
         */
        int getPartitionSize(int pid) const ;

        void print() const ;


            /**
             * total of all partition sizes
             * this is equal to serialized Table data size
             * @return
             */
        int getSerializedTableSize() const ;

        int * serialize();

        /**
         * deserialize TableInfo
         * @param data
         * @return
         */
        static TableInfo * deserialize(int * data, int offset = 0);

        int getSerializedSize();

        template<class TYPE>
        static Table<TYPE> * deserializeTable(TYPE * serializedData, TableInfo * tableInfo, int offset = 0) {
            Table<TYPE> * table = new Table<TYPE>(tableInfo->getTableID());

            int dataIndex = offset;

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
