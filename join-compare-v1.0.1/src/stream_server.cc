#include <grpcpp/grpcpp.h>

#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "comm/config.hpp"
#include "comm/log.hpp"
#include "comm/scope_time.hpp"
#include "serviceImpl/StreamServiceImpl.hpp"
#include "services/stream/stream.grpc.pb.h"

void log_instream(InStream* instream) {
    while (true) {
        while (instream->HasSkew()) {
            auto key = instream->GetSkewKey();
            log_debug("skew_key: %s", key.c_str());
        }
        auto msg = instream->Get();
        for (auto tuple : msg.tuples()) {
            log_debug("%s %s", tuple.key().c_str(), tuple.value().c_str());
        }
        if (msg.empty()) break;
    }
}

void log_bdstream(BDStream* bdstream) {
    int seconds = 5;
    sleep(seconds);
    log_debug("sleep %d seconds done", seconds);
    unordered_map<string, vector<Tuple>> hashmap;
    for (int i = 0; i < 10; ++i) {
        auto key = to_string(i);
        Tuple tuple;
        for (int j = i; j < i + 10; ++j) {
            tuple.set_key(key);
            tuple.set_value(to_string(j));
            hashmap[key].push_back(tuple);
        }
    }
    bdstream->SetSendTuples(&hashmap);
}

void RunServer() {
    StreamInfo info;
    info.set_stream_id(0);
    info.set_type(stream::StreamInfo_StreamType::StreamInfo_StreamType_REMOTE);
    InStream* instream = new InStream(info);
    thread log_instream_thread(log_instream, instream);
    log_instream_thread.detach();

    string client_address = "localhost:50052";
    StreamInfo bd_info;
    bd_info.set_stream_id(1);
    bd_info.set_type(stream::StreamInfo_StreamType::StreamInfo_StreamType_REMOTE);
    bd_info.set_server_address(client_address);
    BDStream* bdstream = new BDStream(bd_info);
    thread log_bdstream_thread(log_bdstream, bdstream);
    log_bdstream_thread.detach();

    std::string server_address("0.0.0.0:50051");
    StreamServiceImpl service;
    service.RegisterStream(instream);
    service.RegisterStream(bdstream);

    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case, it corresponds to an *synchronous* service.
    builder.RegisterService(&service);
    // Finally assemble the server.
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;

    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    server->Wait();
}

int main(int argc, char** argv) {
    Config::Initialize(argc, argv);
    FILE* fp = fopen(Config::g_conf["server_log_file"].c_str(), "w");
    log_set_fp(fp);

    RunServer();

    fclose(fp);
    return 0;
}