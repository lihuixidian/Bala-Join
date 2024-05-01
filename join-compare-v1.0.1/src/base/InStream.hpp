#ifndef INSTREAM_H_
#define INSTREAM_H_

#include "Headers.hpp"
#include "MessageQueue.hpp"

/*
    Input Stream:
    The input for a processor.
*/
class InStream {
   public:
    InStream(const StreamInfo& stream_info)
        : m_stream_id(stream_info.stream_id()),
          m_receive_bytes(0),
          m_type(StreamType(stream_info.type())),
          m_mq(new MessageQueue<BatchTuple>()),
          m_skewkeys_mq(new MessageQueue<string>()) {}
    ~InStream() {
        if (m_mq != nullptr) delete m_mq;
        if (m_skewkeys_mq != nullptr) delete m_skewkeys_mq;
    }

    MessageQueue<string>* GetSkewMQ() { return m_skewkeys_mq; }
    MessageQueue<BatchTuple>* GetMQ() { return m_mq; }

    int GetID() { return m_stream_id; }
    StreamType GetType() { return m_type; }

    BatchTuple Get() { return m_mq->Get(); }
    void Push(const BatchTuple& batch) {
        m_mq->Push(batch);
        assert(m_type == REMOTE);
        addBytes(batch.ByteSizeLong());
    }

    string GetSkewKey() { return m_skewkeys_mq->Get(); }
    void Push(const string& skew_key) {
        assert(m_skewkeys_mq != nullptr);
        m_skewkeys_mq->Push(skew_key);
        assert(m_type == REMOTE);
        addBytes(skew_key.size());
    }
    
    uint64_t GetBytes() { return m_receive_bytes; }
    void addBytes(uint64_t bytes) { m_receive_bytes += bytes; }

    bool HasBatch() { return m_mq->Size() > 0; }
    bool HasSkew() { return m_skewkeys_mq->Size() > 0; }

   private:
    int m_stream_id;                      // Stream ID
    uint64_t m_receive_bytes;             // Amount of received bytes
    StreamType m_type;                    // Stream type
    MessageQueue<BatchTuple>* m_mq;       // Message queue for batch tuples, can only retrieve messages
    MessageQueue<string>* m_skewkeys_mq;  // Message queue for skew keys, can only retrieve messages
};

#endif
