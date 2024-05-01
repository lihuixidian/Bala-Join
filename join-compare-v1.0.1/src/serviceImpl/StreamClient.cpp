#include "StreamClient.hpp"

StreamClient::~StreamClient() {
    if (m_smessage_writer.get() != nullptr) {
        m_smessage_writer->WritesDone();
        Status status = m_smessage_writer->Finish();
        if (status.ok()) {
            // std::cout << "rpc GetStream success" << endl;
        } else {
            // std::cout << "rpc GetStream failed" << endl;
        }
    }

    if (m_bdmessage_writer.get() != nullptr) {
        m_bdmessage_writer->WritesDone();
        Status status = m_bdmessage_writer->Finish();
        if (status.ok()) {
            // std::cout << "rpc GetBDStream success" << endl;
        } else {
            // std::cout << "rpc GetBDStream failed" << endl;
        }
    }
}

void StreamClient::SendSMessage(const SMessage& smessage) {
    if (m_smessage_writer.get() == nullptr) {
        m_smessage_writer.reset(m_stub->GetStream(&m_context, &m_stream_summary).release());
    }
    m_smessage_writer->Write(smessage);
    if (smessage.batch_tuples().empty() && smessage.skew_keys().empty()) {
        m_running = false;
    }
}

void StreamClient::SendBDMessage(const BDMessage& bdmessage) {
    m_mtx.lock();
    if (m_bdmessage_writer.get() == nullptr) {
        m_bdmessage_writer.reset(m_stub->GetBDStream(&m_bdcontext, &m_bdstream_summary).release());
    }
    m_bdmessage_writer->Write(bdmessage);
    m_mtx.unlock();
    if (bdmessage.need_keys().empty() && bdmessage.done_keys().empty() && bdmessage.batch_tuples().empty()) {
        m_running = false;
    }
}

StatisticMsg StreamClient::StartProcessors(const ProcessorsInfo& processor_infos) {
    ClientContext context;
    StatisticMsg statistic_msg;
    Status status = m_stub->StartProcessors(&context, processor_infos, &statistic_msg);
    if (!status.ok()) {
        log_debug("RPC request StartProcessors failed. Request address: %s", context.peer().c_str());
    } else {
        log_debug("RPC request StartProcessors succeeded. Request address: %s", context.peer().c_str());
    }
    return statistic_msg;
}

SMessage StreamClient::RunDistLDSketch(const SMessage& smessage){
    ClientContext context;
    SMessage response;
    unique_ptr<ClientWriter<SMessage>> writer(m_stub->RunDistLDSketch(&context, &response));
    writer->Write(smessage);
    writer->WritesDone();
    Status status = writer->Finish();

    return response;
}