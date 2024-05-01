#include "DataManager.hpp"

// #include <direct.h>
// #include <io.h>

#include <algorithm>
#include <cassert>
#include <fstream>
#include <random>

#include "../comm/log.hpp"
#include "../comm/tools.hpp"

DataManager::DataManager(int small_table_size, int big_table_size, double small_skew_degree, double big_skew_data,
                         int node_nums) {
    m_node_nums = node_nums;
    m_small_table_size = small_table_size;
    m_big_table_size = big_table_size;
    m_big_skew_degree = big_skew_data;
    m_small_skew_degree = small_skew_degree;

    auto remove_zero = [] (string str) {
        int cnt = 0;
        for (int i = str.size()-1; i >= 0; --i) {
            if (str[i] == '0') {
                cnt++;
                continue;
            }
            break;
        }
        str.resize(str.size() - cnt);
        return str;
    };
    m_dir = "../resource/" + to_string(node_nums) + "_" + to_string(small_table_size) + "_" +
            remove_zero(to_string(small_skew_degree)) + "_" + to_string(big_table_size) + "_" + remove_zero(to_string(big_skew_data)) + "/";
}

void DataManager::CheckData() {
    fstream f;
    bool failed = false;
    for (int i = 0; i < m_node_nums; ++i) {
        string small_file = m_dir + to_string(i) + "/small";
        f.open(small_file, ios::in);
        if (!f.is_open()) {
            log_debug("%s does not exist", small_file.c_str());
            failed = true;
            break;
        } else {
            f.close();
        }

        string big_file = m_dir + to_string(i) + "/big";
        f.open(big_file, ios::in);
        if (!f.is_open()) {
            failed = true;
            log_debug("%s does not exist", big_file.c_str());
            break;
        } else {
            f.close();
        }
    }

    if (failed) {
        log_debug("------------------File check failed, data files are incomplete-----------------------");
    }
}

vector<Tuple> DataManager::GetData(const string& file_name) {
    fstream fin(file_name, ios::in);
    if (!fin.is_open()) {
        log_debug("Failed to read %s", file_name.c_str());
    }

    vector<Tuple> data;
    string line;
    Tuple tuple;
    while (getline(fin, line)) {
        auto strs = Tools::Split(line, " ");
        assert(strs.size() == 2);
        tuple.set_key(strs[0]);
        tuple.set_value(strs[1]);
        data.push_back(tuple);
    }
    fin.close();
    assert(!data.empty());

    return data;
}

string DataManager::GetFileName(int nodeID, bool small) {
    return m_dir + to_string(nodeID) + (small ? "/small" : "/big");
}

void DataManager::WriteData() {
    auto write_data = [&](bool small, uint64_t size) {
        double skew_degree = small ? m_small_skew_degree : m_big_skew_degree;
        auto data = this->Generate(size, skew_degree);
        assert(!data.empty());
        log_debug("Generated data size: %ld", data.size());

        vector<string> file_names;
        for (int i = 0; i < m_node_nums; ++i) {
            file_names.push_back(m_dir + to_string(i) + (small ? "/small" : "/big"));
        }

        for (int i = 0; i < m_node_nums; ++i) {
            ofstream fout(file_names[i]);
            if (!fout.is_open()) {
                log_debug("Failed to write data to %s", file_names[i].c_str());
            }
            int count = 0;
            for (int j = i; j < data.size(); j += m_node_nums) {
                auto& item = data[j];
                string line = item.key() + " " + item.value();
                fout << line << endl;
                count++;
            }
            fout.close();
            log_debug("Data written to %s successfully, size: %d.", file_names[i].c_str(), count);
        }
    };

    write_data(true, 1000000);
    write_data(false, 3000000);
}

vector<Tuple> DataManager::Generate(uint64_t data_size, double skew_degree) {
    vector<Tuple> data;

    // Randomly generate data_size tuples
    uniform_int_distribution<unsigned> u1(0, 10000000);
    Tuple tuple;
    for (unsigned i = 0; i < data_size; ++i) {
        tuple.set_key(to_string(u1(m_rand_seed)));
        tuple.set_value(to_string(u1(m_rand_seed)));
        data.push_back(tuple);
    }

    // Transform the dataset to make skewed data account for skew_degree
    uniform_int_distribution<unsigned> u2(0, data_size - 1);
    int skew_size = static_cast<int>(static_cast<double>(data_size) * skew_degree);
    skew_size = abs(skew_size);
    assert(skew_size >= 0);

    vector<string>* select_keys = new vector<string>();
    for (int i = 0; i < 10; ++i) select_keys->push_back(to_string(222320 + i));

    vector<int> select_key_size;
    for (int i = 0; i < 10; ++i) {
        double rate = static_cast<double>(i + 1) / 55.0;  // rate = 1/55, 2/55, 3/55, ... ,10/55
        select_key_size.push_back(static_cast<int>(skew_size * rate));
    }

    unsigned skew_index = 0;
    for (int i = 0; i < 10; ++i) {
        auto skew_key = (*select_keys)[i];
        auto skew_key_size = select_key_size[i];
        auto skew_index_end = skew_index + skew_key_size;

        assert(skew_index_end < data_size);
        for (; skew_index < skew_index_end; ++skew_index) {
            data[skew_index].set_key(skew_key);
        }
    }

    return data;

    // for (int i = 0; i < 20; ++i) {
    //     std::random_shuffle(data.begin(), data.end());
    // }
}