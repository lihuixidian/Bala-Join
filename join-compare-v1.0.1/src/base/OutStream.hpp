#ifndef OUTSTREAM_H_
#define OUTSTREAM_H_

#include "../serviceImpl/StreamClient.hpp"
#include "Headers.hpp"
#include "MessageQueue.hpp"

/*
    Output stream:
    Processor's output
*/
class OutStream {
public:
    OutStream(const StreamInfo& stream_info)
        : m_stream_id(stream_info.stream_id()),
          m_type(StreamType(stream_info.type())),
          m_client(StreamType(stream_info.type()) == StreamType::REMOTE ? new StreamClient(stream_info.server_address())
                                                                        : nullptr),
          m_mq(nullptr),
          m_skewkeys_mq(nullptr) {}
    ~OutStream();

    void Push(const BatchTuple& msg);
    void Push(const string& skew_key);
    void SetMQ(MessageQueue<BatchTuple>* mq, MessageQueue<string>* skewkeys_mq) {
        m_mq = mq;
        m_skewkeys_mq = skewkeys_mq;
    }
    void AssertMQ() { assert(m_mq != nullptr && m_skewkeys_mq != nullptr); }
    StreamType GetType() { return m_type; }
    int GetID() { return m_stream_id; }

private:
    int m_stream_id;                      // Stream ID
    StreamType m_type;                    // Stream type
    StreamClient* m_client;               // Used to send data when the stream is of type REMOTE
    MessageQueue<BatchTuple>* m_mq;       // Used to temporarily store messages when the stream is of type LOCAL
    MessageQueue<string>* m_skewkeys_mq;  // Used to temporarily store messages when the stream is of type LOCAL
    unordered_set<string> m_skew_keys;    // Sent skew_keys
};

#endif
