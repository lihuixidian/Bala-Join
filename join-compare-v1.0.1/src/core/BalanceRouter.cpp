#include "BalanceRouter.hpp"

#include "../comm/config.hpp"
#include "../comm/log.hpp"

double NodeStatistic::CalculateBalance(int id) {
    assert(id >= 0 && id < m_send_tuple_counts->size());
    auto max_size = m_max_size;
    auto min_size = m_min_size;

    // Update the maximum and minimum values
    max_size = max(max_size, (*m_send_tuple_counts)[id] + 1);
    if ((*m_send_tuple_counts)[id] == min_size && m_min_size_count == 1) {
        min_size++;
    }

    return ((double)max_size - (double)min_size) / (double)max_size;
}

void NodeStatistic::Increment(int id) {
    assert(id >= 0 && id < m_send_tuple_counts->size());
    (*m_send_tuple_counts)[id]++;
    // Update the minimum value
    if ((*m_send_tuple_counts)[id] - 1 == m_min_size) {
        m_min_size_count--;
        assert(m_min_size_count >= 0);
        if (m_min_size_count == 0) {
            m_min_size = (*m_send_tuple_counts)[id];
            for (auto count : (*m_send_tuple_counts)) {
                if (count == m_min_size) m_min_size_count++;
            }
        }
    }
    // Update the maximum value
    m_max_size = max(m_max_size, (*m_send_tuple_counts)[id]);
    assert(m_max_size >= m_min_size);
}

void NodeStatistic::Debug() {
    // log_debug("m_send_id : %d", m_send_id);
    string line = "";
    for (auto num : *m_send_tuple_counts) {
        line += to_string(num) + " ";
    }
    log_debug("%s", line.c_str());
}

int SkewValueHelper::GetSendID() {
    // Return first, then update
    int ret_id = m_send_id;
    m_send_counts[ret_id]++;
    m_count++;
    if (m_count % m_update_frequency == 0) {
        int min_size = m_send_counts[m_send_id];
        for (int id = 0; id < m_send_counts.size(); ++id) {
            if (m_send_counts[id] == 0) continue;
            if (m_send_counts[id] < min_size) {
                m_send_id = id;
                min_size = m_send_counts[id];
            }
        }
    }
    return ret_id;
}

void SkewValueHelper::UpdateSendID() {
    while (true) {
        int select_val;
        int select_id = -1;
        for (auto id : m_send_seqs) {
            if (select_id == -1 || m_table->GetSize(id) < select_val) {
                select_id = id;
                select_val = m_table->GetSize(id);
            }
        }

        if (m_table->GetSize(select_id) < m_table->GetSize(m_send_id)) {
            assert(select_id >= 0 && select_id < m_table->Size());
            m_send_id = select_id;
            return;
        }
        if (m_expand_done) return;
        this->expandSeqs();
    }
}

void SkewValueHelper::expandSeqs() {
    // Debug assertion
    if (m_send_seqs.size() >= m_table->Size()) {
        // log_debug("m_send_id : %d", m_send_id);
        string line = "";
        for (auto id : m_send_seqs) line += to_string(id) + " ";
        log_debug("%s", line.c_str());
        m_table->Debug();
    }
    assert(m_send_seqs.size() < m_table->Size());

    // Determine the id to be added
    vector<bool> used(m_table->Size(), false);
    for (auto node_id : m_send_seqs) {
        used[node_id] = true;
    }
    int add_value = m_send_seqs.size();
    auto value = m_skew_value + " " + to_string(add_value++);
    int new_id = CalHashVal(value) % m_table->Size();
    while (used[new_id]) {
        value = m_skew_value + " " + to_string(add_value++);
        new_id = CalHashVal(value) % m_table->Size();
    }

    // Add
    m_send_seqs.push_back(new_id);
    m_notifies.push_back(new_id);
    assert(new_id >= 0 && new_id < m_table->Size());
    m_expand_done = m_send_seqs.size() == m_table->Size();
}

vector<int> SkewValueHelper::GetNotifies() {
    auto res = m_notifies;
    m_notifies.clear();
    return res;
}

BalanceRouter::BalanceRouter(int bucket_nums, NodeStatistic* table) : m_bucket_nums(bucket_nums), m_table(table) {
    assert(Config::g_conf.find("balance_factor") != Config::g_conf.end());
    assert(Config::g_conf.find("frequency_threshold") != Config::g_conf.end());
    m_balance_factor = stod(Config::g_conf["balance_factor"]);
    m_frequency_ratio = stod(Config::g_conf["frequency_threshold"]);
}

BalanceRouter::~BalanceRouter() {
    for (auto& item : m_skew_values) {
        if (item.second != nullptr) delete item.second;
    }
}

vector<int> BalanceRouter::CalDistribute(const Tuple& tuple) {
    auto key = tuple.key();
    auto skewhelper = m_skew_values[key];
    if (skewhelper == nullptr) {
        skewhelper = new SkewValueHelper(key, m_table);
        m_skew_values[key] = skewhelper;
    }
    int send_id = skewhelper->GetSendID();
    if (m_table->GetSize(send_id) > (1.0 / m_frequency_ratio) && m_table->GetSize(send_id) == m_table->GetMaxSize() &&
        m_table->CalculateBalance(send_id) > m_balance_factor) {
        skewhelper->UpdateSendID();
        send_id = skewhelper->GetSendID();
    }
    return vector<int>(1, send_id);
}

vector<int> BalanceRouter::CalNotify(const Tuple& tuple) {
    auto key = tuple.key();
    auto skewhelper = m_skew_values[key];
    if (skewhelper == nullptr) {
        skewhelper = new SkewValueHelper(key, m_table);
        m_skew_values[key] = skewhelper;
    }
    return skewhelper->GetNotifies();
}
