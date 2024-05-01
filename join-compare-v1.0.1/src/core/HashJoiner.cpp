#include "HashJoiner.hpp"

#include <unordered_map>

#include "../base/Histogram.hpp"
#include "../serviceImpl/StreamServiceImpl.hpp"
#include "TableReader.hpp"

HashJoiner::HashJoiner(const HashJoinerInfo &hashjoiner_info, StreamServiceImpl *stream_service) {
    m_stream_service = stream_service;
    m_type = HJType(hashjoiner_info.type());
    m_sfr_helper = nullptr;

    vector<StreamInfo> big_infos;
    for (auto &info : hashjoiner_info.big_infos()) {
        big_infos.push_back(info);
    }
    m_big_input = new Input(big_infos, stream_service);

    vector<StreamInfo> small_infos;
    for (auto &info : hashjoiner_info.small_infos()) {
        small_infos.push_back(info);
    }
    m_small_input = new Input(small_infos, stream_service);

    if (m_type != HJType::FLOWJOIN) {
        for (auto &info : hashjoiner_info.bd_infos()) {
            auto stream = new BDStream(info);
            m_small_bdstreams.push_back(stream);
            m_stream_service->RegisterStream(stream);
        }
    } else {
        int node_nums = hashjoiner_info.bd_infos().size() / 2;
        for (int i = 0; i < node_nums; ++i) {
            auto &info = hashjoiner_info.bd_infos()[i];
            auto stream = new BDStream(info);
            m_small_bdstreams.push_back(stream);
            m_stream_service->RegisterStream(stream);
        }
        for (int i = node_nums; i < hashjoiner_info.bd_infos().size(); ++i) {
            auto &info = hashjoiner_info.bd_infos()[i];
            auto stream = new BDStream(info);
            m_big_bdstreams.push_back(stream);
            m_stream_service->RegisterStream(stream);
        }
    }

    vector<StreamInfo> output_infos;
    for (auto &info : hashjoiner_info.output_infos()) {
        output_infos.push_back(info);
    }
    m_output = new Output(output_infos);

    switch (m_type) {
    case HJType::BASE:
        log_debug("HashJoin type: GraHJ");
        break;

    case HJType::PNR:
        log_debug("HashJoin type: PNR");
        break;

    case HJType::PRPD:
        log_debug("HashJoin type: PRPD");
        break;

    case HJType::BNR:
        log_debug("HashJoin type: BNR");
        break;

    case HJType::FLOWJOIN:
        log_debug("HashJoin type: FLOWJOIN");
        break;

    default:
        assert(false);
        break;
    }
}

HashJoiner::~HashJoiner() {
    if (m_big_input != nullptr) delete m_big_input;
    if (m_small_input != nullptr) delete m_small_input;
    if (m_output != nullptr) delete m_output;
    if (m_sfr_helper != nullptr) delete m_sfr_helper;

    for (auto stream : m_small_bdstreams) {
        m_stream_service->UnRegisterStream(stream);
        if (stream != nullptr) delete stream;
    }
    for (auto stream : m_big_bdstreams) {
        m_stream_service->UnRegisterStream(stream);
        if (stream != nullptr) delete stream;
    }
}

vector<InStream *> HashJoiner::GetInstreams() {
    vector<InStream *> streams;
    auto small_streams = m_small_input->GetStreams();
    auto big_streams = m_big_input->GetStreams();
    streams.insert(streams.end(), small_streams.begin(), small_streams.end());
    streams.insert(streams.end(), big_streams.begin(), big_streams.end());
    return streams;
}

uint64_t HashJoiner::runFlowJoin() {
    log_debug("HashJoiner::runFlowJoin ");
    vector<pair<Tuple, Tuple>> results;
    uint64_t result_size = 0;
    int bucket_nums = m_small_bdstreams.size();
    int local_idx = -1;
    for (int i = 0; i < m_small_bdstreams.size(); ++i) {
        if (m_small_bdstreams[i]->GetType() == StreamType::LOCAL) {
            assert(local_idx == -1);
            local_idx = i;
        }
    }
    assert(local_idx != -1);

    // build
    int build_size = 0;
    for (;;) {
        auto p = m_small_input->Next();
        // Global skewed values
        auto &skew_keys = p.first;
        for (auto &key : skew_keys) {
            m_global_small_skews.insert(key);
        }
        auto &batch = p.second;
        if (batch.empty()) break;
        for (auto &tuple : batch.tuples()) {
            m_hashmap[tuple.key()].push_back(tuple);
            build_size++;
        }
    }
    log_debug("build done. deal size : %d", build_size);
    for (auto bdstream : m_small_bdstreams) {
        bdstream->SetSendTuples(&m_hashmap);
    }
    log_debug("SetSendTuples done.");
    // Set global_small_skews on the local big table reader
    assert(m_local_big_tr.size() > 0);
    for (auto big_tr : m_local_big_tr) {
        big_tr->SetGlobalSmallSkews(&m_global_small_skews);
    }
    // for (auto key : global_small_skews) {
    //     log_debug("small key : %s, count : %ld", key.c_str(), hash_map[key].size());
    // }

    // probe (hash join phase one: process non-skewed values in big table)
    vector<Tuple> big_input;
    unordered_set<string> global_intersect_skews;  // Intersecting skewed values in the big table
    unordered_set<string> global_big_skews;        // Ordinary skewed values in the big table
    int probe_size = 0;
    for (;;) {
        auto p = m_big_input->Next();
        // 1. Classify global skewed values in the big table
        auto &skew_keys = p.first;
        for (auto &key : skew_keys) {
            if (m_global_small_skews.find(key) != m_global_small_skews.end()) {
                // Intersecting skewed values, record first, and calculate SFR allocation later
                global_intersect_skews.insert(key);
            } else {
                // Ordinary skewed values, each node fetches this skewed value (equivalent to broadcasting the value to all nodes)
                global_big_skews.insert(key);
                auto idx = CalHashVal(key) % bucket_nums;
                m_small_bdstreams[idx]->FetchKey(key);
                // log_debug("Single node fetches key: %s", key.c_str());
            }
        }
        // 2. Process non-skewed values in the big table, combine results
        auto &batch = p.second;
        if (batch.empty()) break;
        for (auto &tuple : batch.tuples()) {
            big_input.push_back(tuple);
            probe_size++;
            auto key = tuple.key();
            if (global_big_skews.find(key) != global_big_skews.end()) continue;
            if (m_hashmap.find(key) == m_hashmap.end()) continue;
            for (auto &small_tuple : m_hashmap[key]) {
                // results.push_back({small_tuple, tuple});
                result_size++;
            }
        }
    }
    log_debug("probe (hash join phase one: process non-skewed values in big table) done. deal size: %d", probe_size);
    assert(m_big_bdstreams.size() == m_small_bdstreams.size());
    // assert(m_big_intersect_tuples.size() == global_intersect_skews.size());
    for (auto bdstream : m_big_bdstreams) {
        bdstream->SetSendTuples(&m_big_intersect_tuples);
    }

    // Probe (Hash Join Phase Two: Handle Normal Skewed Values in the Large Table)
    int scratch_size = 0;
    unordered_map<string, vector<Tuple>> fetch_tuples;
    for (auto bdstream : m_small_bdstreams) {
        pair<string, vector<Tuple>> fetch;
        while (bdstream->GetFetchTuples(fetch)) {
            auto &key = fetch.first;
            auto &vec = fetch.second;
            auto &tuples = fetch_tuples[key];
            tuples.insert(tuples.end(), vec.begin(), vec.end());
            scratch_size += vec.size();
        }
    }
    // for (auto key : global_small_skews) {
    //     log_debug("small key : %s, local count : %ld, fetch count : %ld, total count : %ld", key.c_str(), hash_map[key].size(),
    //               fetch_tuples[key].size(), hash_map[key].size() + fetch_tuples[key].size());
    // }
    for (auto &tuple : big_input) {
        auto key = tuple.key();
        if (fetch_tuples.find(key) == fetch_tuples.end()) continue;
        for (auto &small_tuple : fetch_tuples[key]) {
            // results.push_back({small_tuple, tuple});
            result_size++;
        }
    }
    log_debug("probe (hash join phase two: handle normal skewed values in the large table) done. scratch size : %d", scratch_size);

    // Probe (Hash Join Phase Three: Handle Intersecting Skewed Values in the Large Table)
    // Row node fetches the intersecting part from small
    assert(m_sfr_helper != nullptr);
    auto entire_rows = m_sfr_helper->GetEntireRows(local_idx);
    string rows = "";
    for (auto idx : entire_rows) rows += to_string(idx) + " ";
    for (auto key : global_intersect_skews) {
        // log_debug("Row node fetches key : %s, fetch positions : %s, local position : %d", key.c_str(), rows.c_str(), local_idx);
        for (auto idx : entire_rows) {
            m_small_bdstreams[idx]->FetchKey(key);
        }
    }
    for (auto stream : m_small_bdstreams) stream->FetchDone();
    // Column node fetches the intersecting part from big
    auto entire_cols = m_sfr_helper->GetEntireCols(local_idx);
    string cols = "";
    for (auto idx : entire_cols) cols += to_string(idx) + " ";
    for (auto key : global_intersect_skews) {
        // log_debug("Column node fetches key : %s, fetch positions : %s, local position : %d", key.c_str(), cols.c_str(), local_idx);
        for (auto idx : entire_cols) {
            m_big_bdstreams[idx]->FetchKey(key);
        }
    }
    for (auto stream : m_big_bdstreams) stream->FetchDone();
    log_debug("Row and column fetch requests sent");
    unordered_map<string, vector<Tuple>> fetch_small_tuples;
    int scrach_size1 = 0;
    for (auto bdstream : m_small_bdstreams) {
        assert(bdstream != nullptr);
        pair<string, vector<Tuple>> fetch;
        while (bdstream->GetFetchTuples(fetch)) {
            auto &key = fetch.first;
            auto &vec = fetch.second;
            auto &tuples = fetch_small_tuples[key];
            tuples.insert(tuples.end(), vec.begin(), vec.end());
            scrach_size1 += vec.size();
        }
    }
    log_debug("Row fetch scratch1 size : %d", scrach_size1);
    unordered_map<string, vector<Tuple>> fetch_big_tuples;
    int scratch2_size = 0;
    for (auto bdstream : m_big_bdstreams) {
        pair<string, vector<Tuple>> fetch;
        while (bdstream->GetFetchTuples(fetch)) {
            auto &key = fetch.first;
            auto &vec = fetch.second;
            auto &tuples = fetch_big_tuples[key];
            tuples.insert(tuples.end(), vec.begin(), vec.end());
            scratch2_size += vec.size();
        }
    }
    log_debug("Column fetch scratch2 size : %d", scratch2_size);
    for (auto &small_item : fetch_small_tuples) {
        auto key = small_item.first;
        auto &big_tuples = fetch_big_tuples[key];
        for (auto &small_tuple : small_item.second) {
            for (auto &big_tuple : big_tuples) {
                result_size++;
            }
        }
    }

    // log_debug("result_size : %ld", result_size);
    log_debug("HashJoiner::runFlowJoin done");
    return result_size;
}

pair<uint64_t, uint64_t> HashJoiner::CalRecieveBytes() {
    uint64_t recieve_bytes1 = 0;
    auto small_streams = m_small_input->GetStreams();
    for (auto stream : small_streams) {
        if (stream->GetType() == StreamType::LOCAL) assert(stream->GetBytes() == 0);
        recieve_bytes1 += stream->GetBytes();
    }
    auto big_streams = m_big_input->GetStreams();
    for (auto stream : big_streams) {
        if (stream->GetType() == StreamType::LOCAL) assert(stream->GetBytes() == 0);
        recieve_bytes1 += stream->GetBytes();
    }

    uint64_t recieve_bytes2 = 0;
    for (auto stream : m_small_bdstreams) {
        if (stream->GetType() == StreamType::LOCAL) assert(stream->GetBytes() == 0);
        recieve_bytes2 += stream->GetBytes();
    }
    for (auto stream : m_big_bdstreams) {
        if (stream->GetType() == StreamType::LOCAL) assert(stream->GetBytes() == 0);
        recieve_bytes2 += stream->GetBytes();
    }

    return {recieve_bytes1, recieve_bytes2};
}

uint64_t HashJoiner::Run() {
    if (m_type == HJType::FLOWJOIN) return runFlowJoin();

    log_debug("HashJoiner::Run ");
    vector<pair<Tuple, Tuple>> results;
    uint64_t result_size = 0;
    int bucket_nums = m_small_bdstreams.size();

    // build
    int build_size = 0;
    for (;;) {
        auto p = m_small_input->Next();
        auto &batch = p.second;
        if (batch.empty()) break;
        for (auto &tuple : batch.tuples()) {
            m_hashmap[tuple.key()].push_back(tuple);
            build_size++;
        }
    }
    log_debug("build done. deal size : %d", build_size);
    for (auto bdstream : m_small_bdstreams) {
        bdstream->SetSendTuples(&m_hashmap);
    }
    log_debug("SetSendTuples done.");

    // probe (hash join phase one)
vector<Tuple> big_input;
int probe_size = 0;
for (;;) {
    auto p = m_big_input->Next();
    // 1. Fetch the necessary keys if required
    auto &skew_keys = p.first;
    for (auto &key : skew_keys) {
        log_debug("Fetching key: %s", key.c_str());
        auto idx = CalHashVal(key) % bucket_nums;
        m_small_bdstreams[idx]->FetchKey(key);
    }
    // 2. Probe the batch and combine the results
    auto &batch = p.second;
    if (batch.empty()) break;
    for (auto &tuple : batch.tuples()) {
        big_input.push_back(tuple);
        probe_size++;
        auto key = tuple.key();
        if (m_hashmap.find(key) == m_hashmap.end()) continue;
        for (auto &small_tuple : m_hashmap[key]) {
            // results.push_back({small_tuple, tuple});
            result_size++;
        }
    }
}
for (auto stream : m_small_bdstreams) stream->FetchDone();
log_debug("probe (hash join phase one) done. processed size: %d", probe_size);

// probe (hash join phase two)
int scratch_size = 0;
unordered_map<string, vector<Tuple>> fetch_tuples;
for (auto bdstream : m_small_bdstreams) {
    pair<string, vector<Tuple>> fetch;
    while (bdstream->GetFetchTuples(fetch)) {
        auto &tuples = fetch_tuples[fetch.first];
        tuples.insert(tuples.end(), fetch.second.begin(), fetch.second.end());
        scratch_size += fetch.second.size();
        log_debug("Successfully fetched small table skewed value, key: %s, fetch count: %ld", fetch.first.c_str(), fetch.second.size());
    }
}
for (auto &tuple : big_input) {
    auto key = tuple.key();
    if (fetch_tuples.find(key) == fetch_tuples.end()) continue;
    for (auto &small_tuple : fetch_tuples[key]) {
        // results.push_back({small_tuple, tuple});
        result_size++;
    }
}
log_debug("probe (hash join phase two) done. scratch size: %d", scratch_size);

log_debug("HashJoiner::Run done");
return result_size;
}