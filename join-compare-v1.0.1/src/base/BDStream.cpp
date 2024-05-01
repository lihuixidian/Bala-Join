#include "BDStream.hpp"

void BDStream::RecieveBDMessage(BDMessage& bdmessage) {
    // Handle the keys fetched by the remote end
    auto& need_keys = bdmessage.need_keys();
    for (auto& key : need_keys) {
        this->sendKey(key);
    }

    // Handle the keys sent by the remote end (fetched by the local end)
    auto& batch_tuple = bdmessage.batch_tuples();
    for (auto& tuple : batch_tuple.tuples()) {
        auto& key = tuple.key();
        m_partition_tuples[key].push_back(tuple);
    }

    // Handle the keys that the remote end has finished sending
    auto& done_keys = bdmessage.done_keys();
    for (auto& key : done_keys) {
        m_fetch_done_keys.Push(key);
    }

    m_recieve_bytes += bdmessage.ByteSizeLong();
}

void BDStream::sendKey(const string& key) {
    // If the data is not yet ready, place it in the queue first
    if (m_send_tuples == nullptr) {
        m_peer_fetch_keys.Push(key);
        return;
    }
    // Once the data is ready, send it directly
    if (m_type == LOCAL)
        this->sendKeyForLocal(key);
    else
        this->sendKeyForRemote(key);
}

void BDStream::sendKeyForLocal(const string& key) {
    assert(m_send_tuples != nullptr);
    auto iter = m_send_tuples->find(key);
    if (iter != m_send_tuples->end()) {
        auto& send_tuples = iter->second;
        for (auto& tuple : send_tuples) {
            auto& key = tuple.key();
            m_partition_tuples[key].push_back(tuple);
        }
    }
    m_fetch_done_keys.Push(key);
}

void BDStream::sendKeyForRemote(const string& key) {
    assert(m_send_tuples != nullptr);

    BDMessage bdmessage;
    bdmessage.set_stream_id(m_stream_id);
    BatchTuple* batch = bdmessage.mutable_batch_tuples();
    batch->set_empty(true);
    int count = 0;

    auto iter = m_send_tuples->find(key);
    if (iter != m_send_tuples->end()) {
        // Load data in batches and send
        auto& tuples = iter->second;
        for (auto& tuple : tuples) {
            auto add_tuple = batch->add_tuples();
            add_tuple->CopyFrom(tuple);
            batch->set_empty(false);
            count++;

            if (count == BatchSize) {
                m_client->SendBDMessage(bdmessage);
                batch->clear_tuples();
                batch->set_empty(true);
                count = 0;
            }
        }
    }

    bdmessage.add_done_keys(key);
    m_client->SendBDMessage(bdmessage);
}

void BDStream::SetSendTuples(unordered_map<string, vector<Tuple>>* send_tuples) {
    m_send_tuples = send_tuples;
    // Once the data is ready, clear the queue tasks
    while (!m_peer_fetch_keys.Empty()) {
        auto key = m_peer_fetch_keys.Get();
        if (m_type == REMOTE) {
            this->sendKeyForRemote(key);
        } else {
            this->sendKeyForLocal(key);
        }
    }
}

void BDStream::FetchKey(const string& key) {
    if (m_fetch_keys.find(key) != m_fetch_keys.end()) {
        // log_debug("FetchKey : %s done", key.c_str());
        return;
    }
    m_fetch_keys.insert(key);

    if (m_type == REMOTE) {
        BDMessage bdmessage;
        bdmessage.set_stream_id(m_stream_id);
        bdmessage.add_need_keys(key);
        m_client->SendBDMessage(bdmessage);
    } else {
        this->sendKey(key);
        // assert(m_send_tuples != nullptr);
        // this->sendKeyForLocal(key);
    }
}

void BDStream::FetchDone() {
    m_fetch = false;
}

bool BDStream::GetFetchTuples(pair<string, vector<Tuple>>& tuples) {
    while (!m_fetch_keys.empty()) {
        auto key = m_fetch_done_keys.Get();
        auto iter = m_partition_tuples.find(key);
        if (iter == m_partition_tuples.end()) {
            tuples.first = key;
            tuples.second.clear();
        } else {
            tuples = *iter;
        }
        m_fetch_keys.erase(key);
        return true;
    }
    return false;
}

void BDStream::CloseClient() {
    assert(m_client != nullptr);
    assert(m_type == REMOTE);
    BDMessage bdmessage;
    bdmessage.set_stream_id(m_stream_id);
    bdmessage.set_close(true);
    m_client->SendBDMessage(bdmessage);
}
