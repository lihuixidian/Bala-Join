#ifndef BALANCEROUTER_H_
#define BALANCEROUTER_H_

#include "Router.hpp"
#include "../comm/config.hpp"

/*
Node Sending Statistics Class: Data volume sent from a single node to various computing nodes.
*/
class NodeStatistic {
   public:
    NodeStatistic(int num)
        : m_max_size(0), m_min_size(0), m_min_size_count(num), m_send_tuple_counts(new vector<uint64_t>(num, 0)) {}
    ~NodeStatistic() { delete m_send_tuple_counts; }

   public:
    int GetSize(int id) { return (*m_send_tuple_counts)[id]; }
    int GetMaxSize() { return m_max_size; }
    int Size() { return m_send_tuple_counts->size(); }
    void Increment(int id);
    double CalculateBalance(int id);
    void Debug();

   private:
    uint64_t m_max_size;                    // Maximum data volume
    uint64_t m_min_size;                    // Minimum data volume
    int m_min_size_count;                   // Number of minimum data volume
    vector<uint64_t>* m_send_tuple_counts;  // Data volume sent to each node
};

/*
Skew Value Helper Class: Manages a skew value
*/
class SkewValueHelper {
   public:
    SkewValueHelper(const string& value, NodeStatistic* table)
        : m_skew_value(value), m_table(table), m_expand_done(false), m_count(0) {
        m_send_id = CalHashVal(value) % table->Size();
        m_send_seqs.push_back(m_send_id);
        vector<int>(m_table->Size(), 0).swap(m_send_counts);

        // expandSeqs();
        assert(Config::g_conf.find("update_frequency") != Config::g_conf.end());
        m_update_frequency = stoi(Config::g_conf["update_frequency"]);
    }

    // int GetSendID() { return m_send_id; }
    int GetSendID();
    void UpdateSendID();
    vector<int> GetNotifies();

   private:
    void expandSeqs();

   private:
    string m_skew_value;  // Skew value
    bool m_expand_done;   // Expansion completed
    int m_send_id;        // Current node being sent to
    int m_update_frequency;
    uint64_t m_count;     // Counter for this skew value

    vector<int> m_send_seqs;    // Nodes already sent to
    vector<int> m_notifies;     // Node numbers to notify
    vector<int> m_send_counts;  // Number of nodes sent to
    NodeStatistic* m_table;     // Data volume table helper class
};

class BalanceRouter : public Router {
   public:
    BalanceRouter(int bucket_nums, NodeStatistic* table);
    ~BalanceRouter();
    vector<int> CalDistribute(const Tuple& tuple) override;
    vector<int> CalNotify(const Tuple& tuple) override;

   private:
    int m_bucket_nums;                                      // Number of buckets
    double m_balance_factor;                                // Balance factor
    double m_frequency_ratio;                               // Skew threshold ratio
    unordered_map<string, SkewValueHelper*> m_skew_values;  // Skew value helper objects
    NodeStatistic* m_table;                                 // Data volume table helper class
};

#endif