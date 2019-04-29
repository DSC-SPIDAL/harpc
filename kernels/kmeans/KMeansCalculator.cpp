//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

#include <iostream>
#include "../../data_structures/DataStructures.h"
#include "../math/HarpMath.h"
#include "float.h"

using namespace std;
using namespace harp;

namespace harp::kernels {
    template<class TYPE>
    void calculateKMeans(ds::Table<TYPE> *centroids,
                         ds::Table<TYPE> *points,
                         int vectorSize,
                         int iterations) {

        int numOfCentroids = static_cast<int>(centroids->getPartitionCount());
        auto *clusters = new ds::Table<TYPE> *[numOfCentroids];
        for (int i = 0; i < numOfCentroids; i++) {
            clusters[i] = new ds::Table<TYPE>(i);
        }


        for (int i = 0; i < iterations; i++) {
            for (auto p :*points->getPartitions()) {
                int minCentroidIndex = 0;
                auto minDistance = DBL_MAX;
                int currentCentroidIndex = 0;
                for (auto c :*centroids->getPartitions()) {
                    double distance = math::partition::distance(p.second, c.second);
                    if (distance < minDistance) {
                        minDistance = distance;
                        minCentroidIndex = currentCentroidIndex;
                    }
                    currentCentroidIndex++;
                }
                clusters[minCentroidIndex]->addPartition(p.second);
            }
            centroids->clear();

            //new centroids
            for (int c = 0; c < numOfCentroids; c++) {
                auto *newC = math::table::mean(clusters[c], vectorSize);
                centroids->addPartition(newC);
            }

            for (int j = 0; j < numOfCentroids; j++) {
                clusters[j]->clear();
            }
        }
        ds::util::deleteTables(clusters, false, static_cast<int>(centroids->getPartitionCount()));
    }
}
