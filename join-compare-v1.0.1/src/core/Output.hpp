#ifndef OUTPUT_H_
#define OUTPUT_H_

#include "../base/Headers.hpp"
#include "../base/MessageQueue.hpp"
#include "../base/OutStream.hpp"
#include "BalanceRouter.hpp"
#include "Router.hpp"

class Output {
   public:
    Output(const vector<StreamInfo>& outstream_infos);
    ~Output();

    void SendTuple(Tuple& tuple, Router* router, bool skew);
    void Done();

    int GetBucketNums() { return m_streams.size(); }
    int GetLocalIdx();
    vector<OutStream*> GetStreams() { return m_streams; }
    NodeStatistic* GetNodeStatistic() { return m_table; }

    private:
        vector<OutStream *> m_streams; // Output streams for outputting messages
        vector<BatchTuple> m_batches;  // Batches waiting to be output for each output stream
        NodeStatistic *m_table;        // Table for the amount of data output
};

inline Output::Output(const vector<StreamInfo>& outstream_infos) {
    int bucket_nums = outstream_infos.size();
    for (int i = 0; i < bucket_nums; ++i) {
        m_streams.push_back(new OutStream(outstream_infos[i]));
        m_batches.push_back(EmptyBatch());
    }
    m_table = new NodeStatistic(bucket_nums);
}

inline Output::~Output() {
    for (auto stream : m_streams) delete stream;
    delete m_table;
}

inline int Output::GetLocalIdx() {
    int res = -1;
    for (int i = 0; i < m_streams.size(); ++i) {
        if (m_streams[i]->GetType() == StreamType::LOCAL) {
            assert(res == -1);
            res = i;
        }
    }
    return res;
}

inline void Output::SendTuple(Tuple& tuple, Router* router, bool skew) {
    assert(router != nullptr);

    if (skew) {
        auto notifies = router->CalNotify(tuple);
        for (auto idx : notifies) {
            m_streams[idx]->Push(tuple.key());
        }
    }

    auto distribute = router->CalDistribute(tuple);
    for (auto idx : distribute) {
        auto& batch = m_batches[idx];
        auto add_tuple = batch.add_tuples();
        add_tuple->set_key(tuple.key());
        add_tuple->set_value(tuple.value());
        batch.set_empty(false);

        if (batch.tuples_size() == BatchSize) {
            m_streams[idx]->Push(batch);
            batch.clear_tuples();
            batch.set_empty(true);
        }
        if (skew) m_table->Increment(idx);
    }
}

inline void Output::Done() {
    auto empty_batch = EmptyBatch();
    for (int i = 0; i < m_streams.size(); ++i) {
        if (m_batches[i].tuples_size() > 0) {
            m_batches[i].set_empty(false);
            m_streams[i]->Push(m_batches[i]);
        }
        m_streams[i]->Push(empty_batch);
    }
}

#endif