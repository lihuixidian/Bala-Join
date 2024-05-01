#ifndef HEADERS_H_
#define HEADERS_H_

#include <cassert>
#include <condition_variable>
#include <mutex>
#include <string>
#include <vector>

#include "../services/stream/stream.grpc.pb.h"
using stream::BatchTuple;
using stream::BDMessage;
using stream::HashJoinerInfo;
using stream::ProcessorsInfo;
using stream::SMessage;
using stream::StreamInfo;
using stream::TableReaderInfo;
using stream::Tuple;

#define BatchSize 1024

inline BatchTuple EmptyBatch() {
    BatchTuple batch;
    batch.set_empty(true);
    return batch;
}

inline uint64_t CalHashVal(const std::string& str) { return std::_Hash_impl::hash(str.c_str(), str.size()); }

enum StreamType { LOCAL, REMOTE };
using TableReaderRouterType = stream::TableReaderInfo_RouterType;

class Condition {
   public:
    void Wait() {
        std::unique_lock<std::mutex> lck(mtx_);
        condition_.wait(lck);
    }
    void Notify() { condition_.notify_one(); }
    void NotifyAll() { condition_.notify_all(); }

   private:
    std::mutex mtx_;
    std::condition_variable condition_;
};

class SFRhelper {
   public:
    SFRhelper(int n, int rows, int cols) : m_n(n), m_rows(rows), m_cols(cols) { assert(m_n == rows * cols); }
    std::vector<int> GetEntireCols(int index) {
        assert(index < m_n); 
        std::vector<int> ret;
        for (int col_idx = (index % m_cols); col_idx < m_n; col_idx += m_cols) {
            ret.push_back(col_idx);
        }
        return ret;
    }

    std::vector<int> GetEntireRows(int index) {
        assert(index < m_n); 
        std::vector<int> ret;
        int row_idx = index / m_cols;  
        int idx = row_idx * m_cols;   
        for (int i = 0; i < m_cols; ++i) {
            ret.push_back(idx + i);
        }
        return ret;
    }

   private:
    int m_n;
    int m_rows;
    int m_cols;
};

#endif