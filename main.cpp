#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <sstream>
#include <chrono>
#include <thread>
#include "Point.h"
#include "InfluxDBFactory.h"

using namespace std;
using namespace chrono;
using namespace influxdb;

int main() {
    // Write Test Data
    auto time1 = system_clock::now();
    vector<Point> pContents;
    vector<string> pRow;
    string pLine, pWord;
    fstream file ("test.csv", ios::in);
    getline(file, pLine);
    while(getline(file, pLine))
    {
        pRow.clear();
        stringstream str(pLine);
        while(getline(str, pWord, ',')) {
            pRow.push_back(pWord);
        }
        chrono::system_clock::time_point pTime(nanoseconds(std::stoull(pRow[1])));
        Point p(pRow[0]);
        p.addField("internal_quote_id",stoll(pRow[2])).addTag("note",pRow[3]).setTimestamp(pTime);
        pContents.push_back(p);
    }
    file.close();
    cout << pContents.size() << endl;
    auto time2 = system_clock::now();
    auto duration = duration_cast<nanoseconds>(time2 - time1);
    cout << "Time: " << duration.count() * nanoseconds::period::num / double(nanoseconds::period::den) << 's' << endl;

    // Connnect to influxdb
    string dbName = "test";
    auto influxdb = InfluxDBFactory::Get("http://localhost:8086?db=" + dbName);

    // Batch write test
    // Single thread
    influxdb->SingleThreadBatchWrite(pContents, 50000);

    // Multi thread
    auto fun = [](vector<Point> batchContents) {
        int batchPointNum = batchContents.size();
        auto influxdb = InfluxDBFactory::Get("http://localhost:8086?db=test");
        influxdb->batchOf(batchPointNum);
        for(int i = 0; i < batchPointNum; i++) {
            influxdb->write(std::move(batchContents[i]));
        }
        influxdb->flushBuffer();
    };
    vector<thread> threads;
    int pointNum = pContents.size();
    int batchSize = 50000;
    int batchNum = pointNum / batchSize;
    for (int i = 0; i < batchNum + 1; i++) {
        vector<Point> batchContents;
        for (int j = i * batchSize; j < i * batchSize + batchSize; j++) {
            if (j >= pointNum)
                break;
            batchContents.push_back(pContents[j]);
        }
        threads.push_back(thread(fun, batchContents));
    }
    for (int i = 0; i < threads.size(); i++) {
        threads[i].join();
    }
}
