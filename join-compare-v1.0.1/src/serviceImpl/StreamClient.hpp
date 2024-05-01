#ifndef STREAMCLIENT_H_
#define STREAMCLIENT_H_

#include <grpcpp/grpcpp.h>

#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <mutex>
using namespace std;

#include "../base/MessageQueue.hpp"
#include "../comm/config.hpp"
#include "../comm/log.hpp"
#include "../comm/scope_time.hpp"
#include "../services/stream/stream.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientWriter;
using grpc::Status;
using stream::BDMessage;
using stream::SMessage;
using stream::Stream;
using stream::StreamSummary;
using stream::StatisticMsg;
using stream::Tuple;

class StreamClient {
   public:
    StreamClient(const string& server_address)
        : m_running(true),
          m_stub(Stream::NewStub(grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials()))),
          m_smessage_writer(nullptr),
          m_bdmessage_writer(nullptr) {
        m_context.set_wait_for_ready(true);
        m_bdcontext.set_wait_for_ready(true);
    }
    ~StreamClient();

    void SendSMessage(const SMessage& smessage);
    void SendBDMessage(const BDMessage& bdmessage);
    StatisticMsg StartProcessors(const ProcessorsInfo& processor_infos);
    SMessage RunDistLDSketch(const SMessage& smessage);

    bool Running() { return m_running; }

   private:
    bool m_running;
    std::unique_ptr<stream::Stream::Stub> m_stub;

    ClientContext m_context;
    StreamSummary m_stream_summary;
    std::unique_ptr<ClientWriter<SMessage>> m_smessage_writer;
    std::mutex m_mtx;

    ClientContext m_bdcontext;
    StreamSummary m_bdstream_summary;
    std::unique_ptr<ClientWriter<BDMessage>> m_bdmessage_writer;
};

#endif