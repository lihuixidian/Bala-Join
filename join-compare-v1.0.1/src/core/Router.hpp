#ifndef ROUTER_H_
#define ROUTER_H_

#include <unordered_map>
#include <vector>
using namespace std;

#include "../base/Headers.hpp"

enum RouterType { HASH, AVERAGE, NATIVE, BALANCE, FLOWSMALL, FLOWBIG, LDSKETCH };

class Router {
   public:
    // Calculate the node to which a tuple should be distributed
    virtual vector<int> CalDistribute(const Tuple& tuple) = 0;
    // Called when a tuple is skew, calculate the nodes to be notified
    virtual vector<int> CalNotify(const Tuple& tuple) = 0;
};

class HashRouter : public Router {
   public:
    HashRouter(int bucket_nums) : m_bucket_nums(bucket_nums) {}
    vector<int> CalDistribute(const Tuple& tuple) override {
        int bucket_idx = CalHashVal(tuple.key()) % m_bucket_nums;
        return vector<int>(1, bucket_idx);
    }
    vector<int> CalNotify(const Tuple& tuple) override { return {}; }

   private:
    int m_bucket_nums;  // Number of buckets
};

class MirrorRouter : public Router {
   public:
    MirrorRouter(int bucket_nums) {
        for (int i = 0; i < bucket_nums; ++i) {
            m_buckets.push_back(i);
        }
    }
    vector<int> CalDistribute(const Tuple& tuple) override { return m_buckets; }
    vector<int> CalNotify(const Tuple& tuple) override { return {}; }

   private:
    vector<int> m_buckets;  // Assigned bucket numbers
};

class AverageRouter : public Router {
   public:
    AverageRouter(int bucket_nums) : m_bucket_nums(bucket_nums) {
        for (int i = 0; i < bucket_nums; ++i) {
            vector<int> notifies;
            for (int j = 0; j < bucket_nums; ++j) {
                if (j == i) continue;
                notifies.push_back(j);
            }
            m_notifies.push_back(notifies);
        }
    }

    vector<int> CalDistribute(const Tuple& tuple) override {
        int idx = m_assign_idx[tuple.key()];
        m_assign_idx[tuple.key()] = (idx + 1) % m_bucket_nums;
        return vector<int>(1, idx);
    }

    vector<int> CalNotify(const Tuple& tuple) override {
        int bucket_idx = CalHashVal(tuple.key()) % m_bucket_nums;
        return m_notifies[bucket_idx];
    }

   private:
    int m_bucket_nums;                        // Number of buckets
    vector<vector<int>> m_notifies;           // Bucket numbers to notify
    unordered_map<string, int> m_assign_idx;  // Index assigned to key
};

// Keep local, notify single node (notify local node)
class LocalRouter : public Router {
   public:
    LocalRouter(int local_idx, int bucket_nums)
        : m_local_bucket(vector<int>(1, local_idx)), m_bucket_nums(bucket_nums) {}

    vector<int> CalDistribute(const Tuple& tuple) override { return m_local_bucket; }
    vector<int> CalNotify(const Tuple& tuple) override {
        int bucket_idx = CalHashVal(tuple.key()) % m_bucket_nums;
        if (bucket_idx == m_local_bucket[0]) return {};
        return m_local_bucket;
    }

   private:
    int m_bucket_nums;           // Number of buckets
    vector<int> m_local_bucket;  // Index assigned to key
};

// Keep local, notify all nodes
class AllNotifyLocalRouter : public Router {
   public:
    AllNotifyLocalRouter(int local_idx, int bucket_nums)
        : m_local_bucket(vector<int>(1, local_idx)), m_bucket_nums(bucket_nums) {
        for (int i = 0; i < m_bucket_nums; ++i) {
            m_notifies.push_back(i);
        }
    }

    vector<int> CalDistribute(const Tuple& tuple) override { return m_local_bucket; }
    vector<int> CalNotify(const Tuple& tuple) override { return m_notifies; }

   private:
    int m_bucket_nums;           // Number of buckets
    vector<int> m_local_bucket;  // Index assigned to key
    vector<int> m_notifies;      // Bucket numbers to notify
};

// Broadcast to specific nodes, notify all nodes
class FixedMirrorRouter : public Router {
   public:
    FixedMirrorRouter(int bucket_nums, vector<int> buckets) : m_buckets(buckets) {
        for (int i = 0; i < bucket_nums; ++i) {
            m_notifies.push_back(i);
        }
    }

    vector<int> CalDistribute(const Tuple& tuple) override { return m_buckets; }
    vector<int> CalNotify(const Tuple& tuple) override { return m_notifies; }

   private:
    vector<int> m_buckets;   // Assigned bucket numbers
    vector<int> m_notifies;  // Bucket numbers to notify
};

// Notify all nodes, no distribution
class OnlyNotifyRouter : public Router {
   public:
    OnlyNotifyRouter(int bucket_nums) {
        for (int i = 0; i < bucket_nums; ++i) {
            m_notifies.push_back(i);
        }
    }

    vector<int> CalDistribute(const Tuple& tuple) override { return {}; }
    vector<int> CalNotify(const Tuple& tuple) override { return m_notifies; }

   private:
    vector<int> m_notifies;  // Bucket numbers to notify
};

#endif