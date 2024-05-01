#include "Histogram.hpp"

#include <cassert>
#include <fstream>

#include "../comm/config.hpp"
#include "../comm/log.hpp"

Histogram::Histogram(string table_file) {
    assert(Config::g_conf.find("histogram_capacity") != Config::g_conf.end());
    assert(Config::g_conf.find("frequency_threshold") != Config::g_conf.end());
    assert(Config::g_conf.find("dynamic_detect") != Config::g_conf.end());

    m_sample_count = 0;
    m_sample_capacity = stoi(Config::g_conf["histogram_capacity"]);
    m_skew_threshold_ratio = stod(Config::g_conf["frequency_threshold"]);

    m_dynamic_detect = stoi(Config::g_conf["dynamic_detect"]) == 1 ? true : false;
    if (!m_dynamic_detect) {
        log_debug("Dynamic Detection Status: Off");
        auto skew_file = table_file + ".skew";
        fstream fin(skew_file, ios::in);
        string line;
        while (getline(fin, line)) {
            int split_idx = -1;
            for (int i = 0; i < line.size(); ++i) {
                if (line[i] == ',') {
                    split_idx = i;
                    break;
                }
            }
            assert(split_idx != -1);
            auto key = line.substr(0, split_idx);
            auto value = line.substr(split_idx + 1, line.size() - (split_idx + 1));
            m_statistic_skew[key] = stoi(value);
        }
        log_debug("Number of skew keys in local table: %ld", m_statistic_skew.size());
    }
}

void Histogram::debug() {
    int skew_threshold = max((int)(m_sample_count * m_skew_threshold_ratio), 10);

    string line =
        "m_sample_count : " + to_string(m_sample_count) + " m_skew_threshold : " + to_string(skew_threshold) + "\n";
    for (auto& p : m_counts_pair) {
        auto& key = p.first;
        auto count = p.second;
        bool skew = (count - m_init_counts[key]) >= skew_threshold;
        line += key + " : " + to_string(count) + " sample_count : " + to_string(count - m_init_counts[key]) +
                " skew : " + (skew ? "true" : "false") + "       |       ";
    }
    log_debug("%s", line.c_str());
}

bool Histogram::SampleKey(const string& key) {
    if (!m_dynamic_detect) {
        return m_statistic_skew.find(key) != m_statistic_skew.end();
    }

    m_sample_count++;
    int skew_threshold = max((int)(m_sample_count * m_skew_threshold_ratio), 10);

    // Already sampled, increment count
    if (m_counts.find(key) != m_counts.end()) {
        auto index = m_counts[key];
        m_counts_pair[index].second++;
        this->Adjust(index);
        return m_counts_pair[index].second - m_init_counts[key] > skew_threshold;
    }

    // Not at capacity, add a count pair
    if (m_counts.size() < m_sample_capacity) {
        m_counts_pair.push_back({key, 1});
        m_init_counts[key] = 1;
        m_counts[key] = m_counts_pair.size() - 1;
        return false;
    }

    // At capacity, find the key with the smallest count (last pair in m_counts_pair), replace with current key and increment count
    auto& key_count = m_counts_pair.back();
    m_counts.erase(key_count.first);
    m_init_counts.erase(key_count.first);
    m_counts[key] = m_counts_pair.size() - 1;
    key_count.first = key;
    m_init_counts[key] = key_count.second;
    key_count.second++;
    this->Adjust(m_counts_pair.size() - 1);
    return false;
}

void Histogram::Adjust(int index) {
    assert(index >= 0 && index < m_counts_pair.size());

    while (index > 0 && m_counts_pair[index].second >= m_counts_pair[index - 1].second) {
        swap(m_counts_pair[index], m_counts_pair[index - 1]);
        assert(m_counts.find(m_counts_pair[index].first) != m_counts.end() &&
               m_counts.find(m_counts_pair[index - 1].first) != m_counts.end());
        m_counts[m_counts_pair[index].first] = index;
        m_counts[m_counts_pair[index - 1].first] = index - 1;
        --index;
    }
}