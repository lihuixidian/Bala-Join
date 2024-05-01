#include "OutStream.hpp"

OutStream::~OutStream() {
    if (m_client != nullptr) delete m_client;
}

void OutStream::Push(const BatchTuple& msg) {
    if (m_type == StreamType::LOCAL) {
        assert(m_mq != nullptr);
        m_mq->Push(msg);
    } else {
        assert(m_client != nullptr);
        assert(m_client->Running());

        SMessage smessage;
        smessage.set_stream_id(m_stream_id);
        auto batch = smessage.mutable_batch_tuples();
        batch->CopyFrom(msg);
        m_client->SendSMessage(smessage);
    }
}

void OutStream::Push(const string& skew_key) {
    // For multiple identical skew_keys, notify only once
    if (m_skew_keys.find(skew_key) != m_skew_keys.end()) return;
    m_skew_keys.insert(skew_key);

    if (m_type == StreamType::LOCAL) {
        m_skewkeys_mq->Push(skew_key);
    } else {
        assert(m_client != nullptr);
        assert(m_client->Running());

        SMessage smessage;
        smessage.set_stream_id(m_stream_id);
        smessage.add_skew_keys(skew_key);
        m_client->SendSMessage(smessage);
    }
}