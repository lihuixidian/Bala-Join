#include "TableReader.hpp"
#include "HashJoiner.hpp"

#include <fstream>

#include "../base/Histogram.hpp"
using namespace std;

TableReader::TableReader(const TableReaderInfo& tablereader_info) {
    m_id = tablereader_info.id();
    m_tr_type = TableReaderType(tablereader_info.tr_type());
    m_file_name = tablereader_info.file_name();
    m_router_type = RouterType((int)tablereader_info.router_type());
    m_global_small_skews = nullptr;
    m_sfr_helper = nullptr;
    m_local_hjer = nullptr;

    switch (m_router_type) {
        case RouterType::HASH:
            log_debug("tablereader_id : %d, router_type : HASH", m_id);
            break;

        case RouterType::NATIVE:
            log_debug("tablereader_id : %d, router_type : NATIVE", m_id);
            break;

        case RouterType::AVERAGE:
            log_debug("tablereader_id : %d, router_type : AVERAGE", m_id);
            break;

        case RouterType::BALANCE:
            log_debug("tablereader_id : %d, router_type : BALANCE", m_id);
            break;

        case RouterType::FLOWSMALL:
            log_debug("tablereader_id : %d, router_type : FLOWSMALL", m_id);
            break;

        case RouterType::FLOWBIG:
            log_debug("tablereader_id : %d, router_type : FLOWBIG", m_id);
            break;

        case RouterType::LDSKETCH:
            log_debug("tablereader_id : %d, router_type : LDSKETCH", m_id);
            break;

        default:
            break;
    }

    vector<StreamInfo> output_infos;
    for (auto& info : tablereader_info.output_infos()) {
        output_infos.push_back(info);
    }
    m_output = new Output(output_infos);
}

TableReader::~TableReader() {
    if (m_output != nullptr) delete m_output;
    if (m_sfr_helper != nullptr) delete m_sfr_helper;
}

void TableReader::Run() {
    if (m_tr_type == SMALL) {
        log_debug("TableReader::Run : Start reading small table");
    } else {
        log_debug("TableReader::Run : Start reading big table");
    }
    this->readData();

    switch (m_router_type) {
        case RouterType::HASH:
            this->distributeDataForHash();
            break;

        case RouterType::NATIVE:
            this->distributeDataForNative();
            break;

        case RouterType::AVERAGE:
            this->distributeDataForAverage();
            break;

        case RouterType::BALANCE:
            this->distributeDataForBalance();
            break;

        case RouterType::FLOWSMALL:
            this->distributeDataForFlowSmall();
            break;

        case RouterType::FLOWBIG:
            this->distributeDataForFlowBig();
            break;

        case RouterType::LDSKETCH:
            this->distributeDataForLDSketch();
            break;

        default:
            assert(false);
            break;
    }

    if (m_tr_type == SMALL) {
        log_debug("TableReader::Run : small table finished");
    } else {
        log_debug("TableReader::Run : big table finished");
    }
}

void TableReader::readData() {
    fstream fin(m_file_name, ios::in);
    string line;
    Tuple tuple;
    while (getline(fin, line)) {
        int split_idx = -1;
        for (int i = 0; i < line.size(); ++i) {
            if (line[i] == ',') {
                split_idx = i;
                break;
            }
        }
        assert(split_idx != -1);
        tuple.set_key(line.substr(0, split_idx));
        tuple.set_value(line.substr(split_idx + 1, line.size() - (split_idx + 1)));
        m_data.push_back(tuple);
    }
    fin.close();
}

void TableReader::distributeDataForHash() {
    assert(m_output != nullptr);
    Router* router = new HashRouter(m_output->GetBucketNums());

    for (auto& tuple : m_data) {
        m_output->SendTuple(tuple, router, false);
    }
    m_output->Done();

    delete router;
}

void TableReader::distributeDataForNative() {
    assert(m_output != nullptr);
    Router* local_router = new LocalRouter(m_output->GetLocalIdx(), m_output->GetBucketNums());
    Router* hash_router = new HashRouter(m_output->GetBucketNums());
    unordered_map<string, int> key_counts;

    Histogram histogram(m_file_name);
    for (auto& tuple : m_data) {
        if (histogram.SampleKey(tuple.key())) {
            key_counts[tuple.key()]++;
            m_output->SendTuple(tuple, local_router, true);
        } else {
            m_output->SendTuple(tuple, hash_router, false);
        }
    }
    m_output->Done();

    // for (auto item : key_counts) {
    //     log_debug("Big table skewed key: %s frequency: %d, kept locally", item.first.c_str(), item.second);
    // }

    delete local_router;
    delete hash_router;
}

void TableReader::distributeDataForAverage() {
    assert(m_output != nullptr);
    Router* avg_router = new AverageRouter(m_output->GetBucketNums());
    Router* hash_router = new HashRouter(m_output->GetBucketNums());
    unordered_map<string, int> key_counts;

    Histogram histogram(m_file_name);
    for (auto& tuple : m_data) {
        if (histogram.SampleKey(tuple.key())) {
            key_counts[tuple.key()]++;
            m_output->SendTuple(tuple, avg_router, true);
        } else {
            m_output->SendTuple(tuple, hash_router, false);
        }
    }
    m_output->Done();

    // for (auto item : key_counts) {
    //     log_debug("Big table skewed key: %s frequency: %d, evenly distributed", item.first.c_str(), item.second);
    // }

    delete avg_router;
    delete hash_router;
}

void TableReader::distributeDataForBalance() {
    assert(m_output != nullptr);
    Router* balance_router = new BalanceRouter(m_output->GetBucketNums(), m_output->GetNodeStatistic());
    Router* hash_router = new HashRouter(m_output->GetBucketNums());
    unordered_map<string, int> key_counts;

    Histogram histogram(m_file_name);
    for (auto& tuple : m_data) {
        if (histogram.SampleKey(tuple.key())) {
            key_counts[tuple.key()]++;
            m_output->SendTuple(tuple, balance_router, true);
        } else {
            m_output->SendTuple(tuple, hash_router, false);
        }
    }
    m_output->Done();
    m_output->GetNodeStatistic()->Debug();

    // for (auto item : key_counts) {
    //     log_debug("Big table skewed key: %s frequency: %d, balanced distribution", item.first.c_str(), item.second);
    // }

    delete balance_router;
    delete hash_router;
}

void TableReader::distributeDataForLDSketch() {
    // Collect local skew values
    unordered_set<string> skew_keys;
    Histogram histogram(m_file_name);
    for (auto& tuple : m_data) {
        if (histogram.SampleKey(tuple.key())) {
            skew_keys.insert(tuple.key());
        }
    }

    // Collect global skew values
    SMessage msg;
    for (auto key : skew_keys) {
        msg.add_skew_keys(key);
    }
    StreamClient client(Config::g_conf["node0"]);
    auto response = client.RunDistLDSketch(msg);
    unordered_set<string> global_skews;
    for (auto key : response.skew_keys()){
        global_skews.insert(key);
    }

    m_data.clear();
    this->readData();

    // string part_skews = "Local skew values: ";
    // for (auto key : skew_keys) {
    //     part_skews += key + ", ";
    // }
    // string all_skews = "Global skew values: ";
    // for (auto key : global_skews) {
    //     all_skews += key + ", ";
    // }
    // log_debug("%s", part_skews.c_str());
    // log_debug("%s", all_skews.c_str());

    // Distribute like distributeDataForBalance
    assert(m_output != nullptr);
    Router* balance_router = new BalanceRouter(m_output->GetBucketNums(), m_output->GetNodeStatistic());
    Router* hash_router = new HashRouter(m_output->GetBucketNums());
    unordered_map<string, int> key_counts;

    for (auto& tuple : m_data) {
        if (global_skews.find(tuple.key()) != global_skews.end()) {
            key_counts[tuple.key()]++;
            m_output->SendTuple(tuple, balance_router, true);
        } else {
            m_output->SendTuple(tuple, hash_router, false);
        }
    }
    m_output->Done();
    m_output->GetNodeStatistic()->Debug();

    // for (auto item : key_counts) {
    //     log_debug("Big table skewed key: %s frequency: %d, balanced distribution", item.first.c_str(), item.second);
    // }

    delete balance_router;
    delete hash_router;
}

void TableReader::distributeDataForFlowSmall() {
    assert(m_output != nullptr);
    Router* anlocal_router = new AllNotifyLocalRouter(m_output->GetLocalIdx(), m_output->GetBucketNums());
    Router* hash_router = new HashRouter(m_output->GetBucketNums());

    Histogram histogram(m_file_name);
    for (auto& tuple : m_data) {
        if (histogram.SampleKey(tuple.key())) {
            m_output->SendTuple(tuple, anlocal_router, true);
        } else {
            m_output->SendTuple(tuple, hash_router, false);
        }
    }
    m_output->Done();

    delete anlocal_router;
    delete hash_router;
}

void TableReader::distributeDataForFlowBig() {
    assert(m_output != nullptr);
    while (m_global_small_skews == nullptr) {
        m_condition.Wait();
    }
    assert(m_global_small_skews != nullptr);
    assert(m_local_hjer != nullptr);

    int local_idx = m_output->GetLocalIdx();
    Router* anlocal_router = new AllNotifyLocalRouter(local_idx, m_output->GetBucketNums());
    Router* hash_router = new HashRouter(m_output->GetBucketNums());
    Router* mirror_router = new MirrorRouter(m_output->GetBucketNums());
    Router* notify_router = new OnlyNotifyRouter(m_output->GetBucketNums());
    // Broadcast to the columns it belongs to
    // assert(m_sfr_helper != nullptr);
    // vector<int> mirror_buckets(m_sfr_helper->GetEntireCols(local_idx));
    // Router* col_mirror_router = new FixedMirrorRouter(m_output->GetBucketNums(), mirror_buckets);
    // string cols = "";
    // for (auto idx : mirror_buckets) {
    //     cols += to_string(idx) + " ";
    // }
    // log_debug("Broadcast to the columns it belongs to: %s local position: %d", cols.c_str(), local_idx);

    Histogram histogram(m_file_name);
    for (auto& tuple : m_data) {
        auto key = tuple.key();
        if (histogram.SampleKey(key)) {  // skew, notify all nodes uniformly
            if (m_global_small_skews->find(key) != m_global_small_skews->end()) {
                // Intersecting skew values, directly send to the local hash joiner for SFR allocation calculation later
                m_output->SendTuple(tuple, notify_router, true);
                m_local_hjer->AddBigIntersectTuple(tuple);
            } else {
                // Regular skew values, retained locally for direct participation in calculations later
                m_output->SendTuple(tuple, anlocal_router, true);
            }
        } else {  // no skew
            if (m_global_small_skews->find(key) != m_global_small_skews->end()) {
                // Skewed in small table, broadcast it
                m_output->SendTuple(tuple, mirror_router, false);
            } else {
                // Regular values, distributed via hashing
                m_output->SendTuple(tuple, hash_router, false);
            }
        }
    }

    m_output->Done();

    delete anlocal_router;
    delete hash_router;
    delete notify_router;
    delete mirror_router;
}