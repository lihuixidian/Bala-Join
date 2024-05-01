#ifndef STREAMSERVICEIMPL_H_
#define STREAMSERVICEIMPL_H_

#include <grpcpp/grpcpp.h>

#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "../base/BDStream.hpp"
#include "../base/InStream.hpp"
#include "../comm/config.hpp"
#include "../comm/log.hpp"
#include "../comm/scope_time.hpp"
#include "../services/stream/stream.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::Status;
using stream::BatchTuple;
using stream::BDMessage;
using stream::SMessage;
using stream::StatisticMsg;
using stream::Stream;
using stream::StreamSummary;
using stream::Tuple;

class DistServer;

class StreamServiceImpl final : public Stream::Service {
   public:
    StreamServiceImpl() : m_registered(false), m_deal_count(0) {}

    Status StartProcessors(ServerContext* context, const ProcessorsInfo* processors_info,
                           ::stream::StatisticMsg* statistic_msg) override;
    Status GetStream(ServerContext* context, ServerReader<SMessage>* stream, StreamSummary* stream_summary) override;
    Status GetBDStream(ServerContext* context, ServerReader<BDMessage>* stream, StreamSummary* stream_summary) override;
    Status RunDistLDSketch(ServerContext* context, ServerReader<SMessage>* stream, SMessage* response) override;

    void RegisterStream(InStream* stream) { m_instreams[stream->GetID()] = stream; }
    void RegisterStream(BDStream* stream) {
        m_bdstreams[stream->GetID()] = stream;
        log_debug("注册bdstream : %d", stream->GetID());
    }
    void UnRegisterStream(InStream* stream) { m_instreams.erase(stream->GetID()); }
    void UnRegisterStream(BDStream* stream) { m_instreams.erase(stream->GetID()); }

   private:
    void startProcessors(const ProcessorsInfo* processors_info, StatisticMsg* statistic_msg);

    unordered_map<int, InStream *> m_instreams; // Unidirectional stream for GetStream service
    unordered_map<int, BDStream *> m_bdstreams; // Bidirectional stream for GetBDStream service

    bool m_registered;
    Condition m_condition;

    unordered_set<string> m_ld_skews; // Global skew values for ld-sketch
    int m_deal_count;                 // Counter for obtaining node skew values, completion upon reaching number of nodes
    mutex m_ld_mtx;                   // Lock for ld-sketch
    Condition m_ld_condition;         // Condition variable for ld-sketch
};

#endif