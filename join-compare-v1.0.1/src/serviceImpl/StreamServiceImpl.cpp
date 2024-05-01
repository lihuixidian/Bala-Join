#include "StreamServiceImpl.hpp"

#include <future>
#include <thread>
#include <vector>

#include "../core/DistServer.hpp"
#include "../core/HashJoiner.hpp"
#include "../core/TableReader.hpp"

Status StreamServiceImpl::GetStream(ServerContext* context, ServerReader<SMessage>* stream,
                                    StreamSummary* stream_summary) {
    // log_debug("Local rpc task GetStream requested, request address: %s", context->peer().c_str());
    while (!m_registered) {
        m_condition.Wait();
    }

    // Get the instream to operate on
    SMessage smessage;
    stream->Read(&smessage);
    auto stream_id = smessage.stream_id();
    auto instream = m_instreams[stream_id];
    if (instream == nullptr) {
        log_debug("instream id : %d nullptr", stream_id);
        assert(instream != nullptr);
    }

    // Process data
    int count = 0;
    while (true) {
        for (auto& skew_key : smessage.skew_keys()) {
            instream->Push(skew_key);
        }
        instream->Push(smessage.batch_tuples());
        count++;

        if (smessage.batch_tuples().empty() && smessage.skew_keys().empty()) break;
        if (!stream->Read(&smessage)) break;
    }

    stream_summary->set_count(count);
    // log_debug("Execute rpc task GetStream successfully, request address: %s succeeded, responded %d times ", context->peer().c_str(), count);
    return Status::OK;
}

Status StreamServiceImpl::GetBDStream(ServerContext* context, ServerReader<BDMessage>* stream,
                                      StreamSummary* stream_summary) {
    log_debug("Local rpc task GetBDStream requested, request address: %s", context->peer().c_str());
    while (!m_registered) {
        m_condition.Wait();
    }

    // Get the bdstream to operate on
    BDMessage bdmessage;
    stream->Read(&bdmessage);
    auto stream_id = bdmessage.stream_id();
    auto bdstream = m_bdstreams[stream_id];
    if (bdstream == nullptr) {
        log_debug("bdstream id : %d nullptr", stream_id);
        assert(bdstream != nullptr);
    }

    // Process data
    int count = 0;
    while (true) {
        bdstream->RecieveBDMessage(bdmessage);
        count++;

        if (!stream->Read(&bdmessage)) break;
    }

    stream_summary->set_count(count);
    log_debug("Execute rpc task GetBDStream successfully. Request address: %s succeeded, responded %d times ", context->peer().c_str(), count);
    return Status::OK;
}

Status StreamServiceImpl::StartProcessors(ServerContext* context, const ProcessorsInfo* processors_info,
                                          ::stream::StatisticMsg* statistic_msg) {
    log_debug("Local rpc task StartProcessors requested, request address: %s", context->peer().c_str());

    this->startProcessors(processors_info, statistic_msg);

    log_debug(
        "Execute rpc task StartProcessors successfully. Request address: %s, generated answer count : %ld, received bytes from TableReader : %ld, fetched bytes : %ld",
        context->peer().c_str(), statistic_msg->result_counts(), statistic_msg->network_phase1(),
        statistic_msg->network_phase2());
    return Status::OK;
}

void StreamServiceImpl::startProcessors(const ProcessorsInfo* processors_info, StatisticMsg* statistic_msg) {
    auto processors = DistServer::SetupProcessors(processors_info, this);
    log_debug("register done.");
    m_registered = true;
    m_condition.NotifyAll();

    // Asynchronously start each operator
    auto trs = processors.first;
    auto hjer = processors.second;
    vector<thread*> tr_threads;
    for (auto tr : trs) {
        auto run_func = std::bind(&TableReader::Run, tr);
        tr_threads.push_back(new thread(run_func));
    }
    auto hj_run = std::bind(&HashJoiner::Run, hjer);
    auto hj_result = std::async(std::launch::async, hj_run);
    for (int i = 0; i < tr_threads.size(); ++i) {
        tr_threads[i]->join();
    }
    auto result_count = hj_result.get();
    auto hj_recieve_bytes = hjer->CalRecieveBytes();

    statistic_msg->set_result_counts(result_count);
    statistic_msg->set_network_phase1(hj_recieve_bytes.first);
    statistic_msg->set_network_phase2(hj_recieve_bytes.second);
}

Status StreamServiceImpl::RunDistLDSketch(ServerContext* context, ServerReader<SMessage>* stream, SMessage* response) {
    log_debug("Local rpc task RunDistLDSketch requested, request address: %s", context->peer().c_str());

    vector<string> skew_keys;
    SMessage msg;
    while (stream->Read(&msg)) {
        for (auto key : msg.skew_keys()) {
            skew_keys.push_back(key);
        }
    }
    // log_debug("Received %ld skewed values", skew_keys.size());

    m_ld_mtx.lock();
    if (m_ld_skews.empty()) {
        for (auto key : skew_keys) {
            m_ld_skews.insert(key);
        }
    } else {
        unordered_set<string> intersect_keys;
        for (auto key : skew_keys) {
            if (m_ld_skews.find(key) != m_ld_skews.end()) {
                intersect_keys.insert(key);
            }
        }
        m_ld_skews.swap(intersect_keys);
    }
    m_deal_count++;
    m_ld_mtx.unlock();

    int node_nums = stoi(Config::g_conf["node_nums"]);
    if (m_deal_count < node_nums) {
        m_ld_condition.Wait();
    } else {
        m_ld_condition.NotifyAll();
    }

    for (auto key : m_ld_skews) {
        response->add_skew_keys(key);
    }

    log_debug("Execute rpc task RunDistLDSketch successfully. Request address: %s succeeded ", context->peer().c_str());
    return Status::OK;
}
