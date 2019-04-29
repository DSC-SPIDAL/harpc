#include <iostream>
#include "../../data_structures/DataStructures.h"
#include "../../worker/Worker.h"
#include <time.h>
#include "../generators/DataGenerator.h"
#include "../../kernels/HarpKernels.h"
#include <fstream>
#include <chrono>
#include <iomanip>
#include <thread>
#include "future"

#include "timing.h"

#include "print.h"
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>

namespace fs = boost::filesystem;

using namespace harp;
using namespace harp::ds::util;
using namespace harp::util::timing;
using namespace std::chrono;
using namespace std;

bool debugCondition(int workerId) {
    return workerId == 1;
}

int tagCounter = 0;

const int TIME_BEFORE_SERIAL = tagCounter++;
const int TIME_AFTER_SERIAL = tagCounter++;

const int TIME_BEFORE_WAIT = tagCounter++;
const int TIME_AFTER_WAIT = tagCounter++;

const int TIME_PARALLEL_TOTAL_START = tagCounter++;
const int TIME_PARALLEL_TOTAL_END = tagCounter++;

const int TIME_ASYNC_ROTATE_BEGIN = tagCounter++;
const int TIME_ASYNC_ROTATE_END = tagCounter++;

class KMeansWorker : public harp::Worker {

    void execute(com::Communicator *comm, int argc, char *argv[]) {

        int iterations = 5;
        int numOfCentroids = 5;
//        int vectorSize = 1000000;
        int vectorSize = 10000;
        int numOfVectors = 100;

        double serialDuration = 0;

        cout << "worker: " << comm->getWorkerId() << " is starting.." << endl;

        string dirName = "/tmp/harp/kmeans";

        if (workerId == 0) {
            // make directory if does not exist

            if(!fs::is_directory(dirName)) {
                cout << "directory " << dirName << " does not exist. Will try to create." << endl;
                bool dirCreated = fs::create_directories(dirName);
                if(dirCreated)
                    cout << "directory " << dirName << " created." << endl;
                else {
                    cout << "directory " << dirName << " can not be created" << endl;
                    return;
                }
            }
            else
                cout << "directory " << dirName << " already exists. " << endl;

            //generate data only if doesn't exist
            ifstream censtream(dirName + "/centroids");
            if (!censtream.good()) {
                printf("Generating data in node 0\n");
                util::generateKMeansData(dirName, numOfVectors, vectorSize, worldSize, numOfCentroids);
            }

            cout << "data generated" << endl;

            //running non-distributed version in node 0
            auto *points = new harp::ds::Table<double>(0);
            for (int i = 0; i < worldSize; i++) {
                util::readKMeansDataFromFile(dirName + to_string(i), vectorSize, points,
                                             static_cast<int>(points->getPartitionCount()));
            }

            cout << "partition count: " << points->getPartitionCount() << endl;

            auto *centroids = new harp::ds::Table<double>(1);
            util::readKMeansDataFromFile("/tmp/harp/kmeans/centroids", vectorSize, centroids);

//            util::print::printTable(centroids);

            record(TIME_BEFORE_SERIAL);
            kernels::calculateKMeans(centroids, points, vectorSize, iterations);
            record(TIME_AFTER_SERIAL);

            cout << "Time for serial calculation: " << diff(TIME_BEFORE_SERIAL, TIME_AFTER_SERIAL) << endl;

//            util::print::printTable(centroids);
//            util::print::printPartition(centroids->getPartition(0));

            deleteTable(points, true);
            deleteTable(centroids, true);
        }

        cout << "Worker " << comm->getWorkerId() << " is at the first barrier." << endl;

        comm->barrier();

        //////////////////////////////////////////////////////////
        //running distributed version

        //load centroids
        auto *centroids = new harp::ds::Table<double>(1);

        //todo load only required centroids
        util::readKMeansDataFromFile(dirName + "/centroids", vectorSize, centroids);

        if (workerId == 0) {
            cout << "initial centroid table: ";
            centroids->printTableSummary();
        }

        auto *myCentroids = new ds::Table<double>(1);
        for (auto c : *centroids->getPartitions()) {
            if (c.first % worldSize == workerId) {
                //modifying centroids to hold count
                auto *data = c.second->getData();
                auto *newDataWithCount = new double[c.second->getSize() + 1];

                for (int x = 0; x < c.second->getSize(); x++) {
                    newDataWithCount[x] = data[x];
                }
                newDataWithCount[c.second->getSize()] = 0;

                myCentroids->addPartition(
                        new ds::Partition<double>(c.first, newDataWithCount, c.second->getSize() + 1));
            } else {
                delete c.second;
            }
        }

        //load points
        auto *points = new ds::Table<double>(0);
        util::readKMeansDataFromFile(dirName + to_string(workerId), vectorSize, points);

        record(TIME_PARALLEL_TOTAL_START);

        for (int it = 0; it < iterations; it++) {
            auto *minDistances = new double[points->getPartitionCount()];
            auto *closestCentroid = new int[points->getPartitionCount()];

            bool firstRound = true;// to prevent minDistance array initialization requirement

            //determining closest

            int cen = 0;
            while (cen < numOfCentroids && myCentroids->hasNext(true)) {
                auto *nextCent = myCentroids->nextPartition();
                for (auto p:*points->getPartitions()) {
                    double distance = math::partition::distance(p.second, 0, p.second->getSize(),
                                                                nextCent, 0, nextCent->getSize() - 1);
                    if (firstRound || distance < minDistances[p.first]) {
                        minDistances[p.first] = distance;
                        closestCentroid[p.first] = nextCent->getId();
                    }
                }
                firstRound = false;
                cen++;
                //std::cout << "Calling rotate on " << nextCent->getId() << endl;
                record(TIME_ASYNC_ROTATE_BEGIN);
                comm->asyncRotate(myCentroids, nextCent->getId());
                record(TIME_ASYNC_ROTATE_END);
                diff(TIME_ASYNC_ROTATE_BEGIN, TIME_ASYNC_ROTATE_END, true);
            }

            //wait for async communications to complete
            record(TIME_BEFORE_WAIT);
            comm->wait();
            record(TIME_AFTER_WAIT);

            //std::cout << "Wait time 1st rot : " << diff(TIME_BEFORE_WAIT, TIME_AFTER_WAIT, true) << endl;

            ds::util::resetTable<double>(myCentroids, 0);

            //building new centroids
            cen = 0;
            while (cen < numOfCentroids && myCentroids->hasNext(true)) {
                auto *nextCent = myCentroids->nextPartition();
                auto *cdata = nextCent->getData();
                for (auto p:*points->getPartitions()) {
                    if (closestCentroid[p.first] == nextCent->getId()) {
                        cdata[nextCent->getSize() - 1]++;
                        auto *pdata = p.second->getData();
                        for (int i = 0; i < p.second->getSize(); i++) {
                            cdata[i] += pdata[i];
                        }
                    }
                }
                cen++;
                record(TIME_ASYNC_ROTATE_BEGIN);
                comm->asyncRotate(myCentroids, nextCent->getId());
                record(TIME_ASYNC_ROTATE_END);
                diff(TIME_ASYNC_ROTATE_BEGIN, TIME_ASYNC_ROTATE_END, true);
            }

            record(TIME_BEFORE_WAIT);
            comm->wait();
            record(TIME_AFTER_WAIT);

            //cout << "Wait time 2nd rot : " << diff(TIME_BEFORE_WAIT, TIME_AFTER_WAIT, true) << endl;

            //calculating average
            for (auto c:*myCentroids->getPartitions()) {
                auto *data = c.second->getData();
                for (int j = 0; j < vectorSize; j++) {
                    data[j] /= data[vectorSize];
                }
            }

            delete[] minDistances;
            delete[] closestCentroid;
        }
        record(TIME_PARALLEL_TOTAL_END);
        cout << "Parallel Calculation time: " << diff(TIME_PARALLEL_TOTAL_START, TIME_PARALLEL_TOTAL_END) << endl;
        if (workerId == 0) {
            cout << "Speedup: " << diff(TIME_BEFORE_SERIAL, TIME_AFTER_SERIAL) /
                                         diff(TIME_PARALLEL_TOTAL_START, TIME_PARALLEL_TOTAL_END) << endl;

            cout << "Avg async rotation time: " << average(TIME_ASYNC_ROTATE_BEGIN, TIME_ASYNC_ROTATE_END) << endl;
            cout << "Avg Wait time: " << average(TIME_BEFORE_WAIT, TIME_AFTER_WAIT) << endl;
            cout << "Total Communication time : " << total(11, 12) << endl;
            cout << "Total Computation time : " << diff(TIME_PARALLEL_TOTAL_START, TIME_PARALLEL_TOTAL_END) -
                                                        total(TIME_ASYNC_ROTATE_BEGIN, TIME_ASYNC_ROTATE_END) -
                                                        diff(TIME_BEFORE_WAIT, TIME_AFTER_WAIT) << endl;

//            util::print::printTable(myCentroids);
            //printPartition(centroids->getPartition(0));
        }


        comm->barrier();
        //printTable(myCentroids);
        //printPartition(centroids->getPartition(0));

        deleteTable(myCentroids, true);
        deleteTable(points, true);
    }
};


int main(int argc, char *argv[]) {
    KMeansWorker kMeansWorker;
    kMeansWorker.init(argc, argv);
    kMeansWorker.setCommThreads(4);
    kMeansWorker.start();
    return 0;
}