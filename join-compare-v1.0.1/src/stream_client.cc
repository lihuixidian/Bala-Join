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
#include "serviceImpl/StreamServiceImpl.hpp"
#include "services/stream/stream.grpc.pb.h"

void RunTestStream()
{
    // Remote Stream
    string server_address = "localhost:50051";
    StreamInfo info;
    info.set_stream_id(0);
    info.set_type(stream::StreamInfo_StreamType::StreamInfo_StreamType_REMOTE);
    info.set_server_address(server_address);
    OutStream *outstream = new OutStream(info);

    BatchTuple batch;
    batch.set_empty(false);
    for (int i = 0; i < 10; ++i)
    {
        auto tmp = batch.add_tuples();
        tmp->set_key(to_string(i));
        tmp->set_value(to_string(i));
    }
    string skew_key = "duguyd";
    outstream->Push(skew_key);
    outstream->Push(batch);

    // Local Stream
    // OutStream* outstream = new OutStream(0, LOCAL);
    // InStream* instream = new InStream(0, LOCAL);
    // outstream->SetMQ(instream->GetMQ());
    // outstream->Push(batch);
    // auto b = instream->Get();
    // for (auto t : b.tuples()) {
    //     log_debug("key : %s, value : %s", t.key().c_str(), t.value().c_str());
    // }
}

void log_bdstream(BDStream *bdstream)
{
    log_debug("log_bdstream...");
    string key = "1";
    bdstream->FetchKey(key);

    while (true)
    {
        pair<string, vector<stream::Tuple>> fetch_data;
        if (!bdstream->GetFetchTuples(fetch_data))
            break;
        auto key = fetch_data.first;
        auto &tuples = fetch_data.second;
        for (auto &tuple : tuples)
        {
            log_debug("key : %s  value : %s", tuple.key().c_str(), tuple.value().c_str());
        }
    }
    log_debug("log_bdstream done");
}

void RunTestBDStream()
{
    // Remote Stream Test
    string server_address = "localhost:50051";
    StreamInfo info;
    info.set_stream_id(1);
    info.set_type(stream::StreamInfo_StreamType::StreamInfo_StreamType_REMOTE);
    info.set_server_address(server_address);
    BDStream *bdstream = new BDStream(info);
    thread log_bdstream_thread(log_bdstream, bdstream);
    log_bdstream_thread.detach();

    StreamServiceImpl service;
    service.RegisterStream(bdstream);

    ServerBuilder builder;
    std::string client_address("0.0.0.0:50052");
    builder.AddListeningPort(client_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << client_address << std::endl;
    server->Wait();

    // Local Stream Test
    // StreamInfo info;
    // info.set_stream_id(1);
    // info.set_type(stream::StreamInfo_StreamType::StreamInfo_StreamType_LOCAL);
    // BDStream* bdstream = new BDStream(info);
    // bdstream->SetBDStream(bdstream);

    // thread log_bdstream_thread(log_bdstream, bdstream);
    // int seconds = 5;
    // sleep(seconds);
    // log_debug("sleep %d seconds done", seconds);

    // unordered_map<string, vector<Tuple>> hashmap;
    // for (int i = 0; i < 10; ++i) {
    //     auto key = to_string(i);
    //     Tuple tuple;
    //     for (int j = i; j < i + 10; ++j) {
    //         tuple.set_key(key);
    //         tuple.set_value(to_string(j));
    //         hashmap[key].push_back(tuple);
    //     }
    // }
    // bdstream->SetSendTuples(&hashmap);

    // // thread log_bdstream_thread(log_bdstream, bdstream);
    // log_bdstream_thread.join();
}

int main(int argc, char **argv)
{
    Config::Initialize(argc, argv);
    FILE *fp = fopen(Config::g_conf["client_log_file"].c_str(), "w");
    log_set_fp(fp);

    // RunTestStream();
    RunTestBDStream();

    fclose(fp);
    return 0;
}
