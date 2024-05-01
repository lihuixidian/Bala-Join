#include <grpcpp/grpcpp.h>

#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
using namespace std;

#include "base/BDStream.hpp"
#include "base/OutStream.hpp"
#include "comm/config.hpp"
#include "comm/log.hpp"
#include "comm/scope_time.hpp"
#include "comm/tools.hpp"
#include "core/DistServer.hpp"
#include "serviceImpl/StreamServiceImpl.hpp"
#include "services/stream/stream.grpc.pb.h"

void check_data() {
    double small_skew = stod(Config::g_conf["small_skew_degree"]);
    double big_skew = stod(Config::g_conf["big_skew_degree"]);
    int node_nums = stoi(Config::g_conf["node_nums"]);
    int small_size = stoi(Config::g_conf["small_table_size"]);
    int big_size = stoi(Config::g_conf["big_table_size"]);
    DataManager* data_manager = new DataManager(small_size, big_size, small_skew, big_skew, node_nums);
    data_manager->CheckData();
    delete data_manager;

    log_debug("check_data done.");
}

int main(int argc, char** argv) {
    string nodeID = argv[2];
    Config::Initialize(argc, argv);
    string log_file = Config::g_conf["server_log_file"] + "_" + nodeID;
    FILE* fp = fopen(log_file.c_str(), "w");
    log_set_fp(fp);

    // First, check if the data files are complete.
    check_data();

    bool gateway = argc > 3;
    auto server_addr = Config::g_conf[nodeID];

    // Start a distributed server (Constructor: launches a gRPC server to listen for remote calls).
    DistServer* server = new DistServer(server_addr);
    if (gateway) { 
        // If this is a gateway node, generate a distributed execution plan and send it to all nodes for execution.
        server->StartDistPlan();
    }
    delete server; // This will block here (in the destructor) unless the task has finished executing.

    fclose(fp);
    return 0;
}
