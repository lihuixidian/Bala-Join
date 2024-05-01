#ifndef DISTSERVER_H_
#define DISTSERVER_H_

#include <thread>

#include "TableReader.hpp"
#include "HashJoiner.hpp"
#include "../base/DataManager.hpp"
#include "../serviceImpl/StreamServiceImpl.hpp"
using namespace std;

class DistServer {
   public:
    static pair<vector<TableReader*>, HashJoiner*> SetupProcessors(const ProcessorsInfo* processors_info, StreamServiceImpl* service);

   public:
    DistServer(const string& server_address);
    ~DistServer();

    void Run();
    void StartDistPlan();

   private:
    vector<ProcessorsInfo> generateDistPlan(HJType hj_type);
    pair<TableReaderRouterType, TableReaderRouterType> getDistType(HJType hj_type);

   private:
    string m_server_address;
    StreamServiceImpl* m_service;
    DataManager* m_data_manager;
    thread* m_run_thread;
};

#endif
