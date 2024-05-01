#ifndef BDSTREAM_H_
#define BDSTREAM_H_

#include <unordered_map>
#include <unordered_set>

#include "../serviceImpl/StreamClient.hpp"
#include "../services/stream/stream.grpc.pb.h"
#include "Headers.hpp"
#include "MessageQueue.hpp"

using stream::BDMessage;

// BDStream includes both the sending and receiving ends:
// - The sending end is actively controlled by BDStream (Data to be sent by the sender: keys to be pulled by this end, data of the keys to be pulled by the other end, and the keys that have been pushed to the other end)
// - The receiving end is passively controlled by the GRPC thread (Data received by the receiver: keys to be pulled by the other end, data of the keys pulled by this end, and the keys that this end has finished pulling)
class BDStream
{
public:
    BDStream(const StreamInfo &stream_info)
        : m_stream_id(stream_info.stream_id()),
          m_type(StreamType(stream_info.type())),
          m_receive_bytes(0),
          m_client(StreamType(stream_info.type()) == StreamType::REMOTE ? new StreamClient(stream_info.server_address())
                                                                        : nullptr),
          m_fetch(true),
          m_send_tuples(nullptr) {}
    ~BDStream()
    {
        if (m_client != nullptr)
            delete m_client;
    }

public:
    int GetID() { return m_stream_id; }
    StreamType GetType() { return m_type; }
    uint64_t GetBytes() { return m_receive_bytes; }

    void FetchKey(const string &key);
    void FetchDone();
    void SetSendTuples(unordered_map<string, vector<Tuple>> *send_tuples); // Set the data to be sent
    void ReceiveBDMessage(BDMessage &bdmessage);                           // Process BDMessage received by the GRPC thread
    bool GetFetchTuples(pair<string, vector<Tuple>> &tuples);              // Return a successfully fetched key and its tuples

    void CloseClient();

private:
    // Sending: data of the keys that the other end wants to fetch, and the keys that have been pushed to the other end
    void sendKey(const string &key);
    void sendKeyForLocal(const string &key);
    void sendKeyForRemote(const string &key);

private:
    int m_stream_id;          // Stream ID
    StreamType m_type;        // Stream Type
    uint64_t m_receive_bytes; // Number of bytes received by the stream
    StreamClient *m_client;   // Stream client used to send data when the stream is of type REMOTE
    bool m_fetch;             // Flag indicating whether to fetch keys

    MessageQueue<string> m_peer_fetch_keys;                  // Keys that the other end wants to fetch
    unordered_set<string> m_fetch_keys;                      // Keys that this end wants to fetch
    MessageQueue<string> m_fetch_done_keys;                  // Keys that this end has finished fetching
    unordered_map<string, vector<Tuple>> m_partition_tuples; // Tuples fetched (after partitioning)
    unordered_map<string, vector<Tuple>> *m_send_tuples;     // Data to be sent (entire small table)
};

#endif
