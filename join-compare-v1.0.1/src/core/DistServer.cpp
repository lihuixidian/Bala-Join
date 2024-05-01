#include "DistServer.hpp"

#include <grpcpp/grpcpp.h>

#include <future>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "../comm/config.hpp"
#include "../comm/log.hpp"
#include "../comm/scope_time.hpp"
#include "../comm/tools.hpp"

DistServer::DistServer(const string &server_address)
    : m_server_address(server_address), m_service(new StreamServiceImpl()) {
    double small_degree = stod(Config::g_conf["small_skew_degree"]);
    double big_degree = stod(Config::g_conf["big_skew_degree"]);
    int node_nums = stoi(Config::g_conf["node_nums"]);
    int small_size = stoi(Config::g_conf["small_table_size"]);
    int big_size = stoi(Config::g_conf["big_table_size"]);
    m_data_manager = new DataManager(small_size, big_size, small_degree, big_degree, node_nums);

    // Start the grpc server
    auto run_func = std::bind(&DistServer::Run, this);
    m_run_thread = new thread(run_func);
}

DistServer::~DistServer() {
    m_run_thread->join();
    if (m_service != nullptr) delete m_service;
    if (m_data_manager != nullptr) delete m_data_manager;
    if (m_run_thread != nullptr) delete m_run_thread;
}

void DistServer::Run() {
    auto strs = Tools::Split(m_server_address, ":");
    auto server_addr = "0.0.0.0:" + strs[1];

    ServerBuilder builder;
    builder.AddListeningPort(server_addr, grpc::InsecureServerCredentials());
    builder.RegisterService(m_service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    log_debug("Server listening on %s", server_addr.c_str());

    server->Wait();
}

// Only the gateway node will execute this
void DistServer::StartDistPlan() {
    // Generate distributed execution plan (including the execution plans for all nodes)
    int node_nums = stoi(Config::g_conf["node_nums"]);
    int node_nums = stoi(Config::g_conf["node_nums"]);
    HJType type = HJType(stoi(Config::g_conf["hj_type"]));
    auto distplan = this->generateDistPlan(type);

    // Send the execution plan to each node and execute remotely (Implementation: grpc task (StartProcessors remote call))
    vector<future<StatisticMsg>> results;
    vector<StreamClient *> clients;
    for (int i = 0; i < node_nums; ++i) {
        auto server_address = Config::g_conf["node" + to_string(i)];
        StreamClient *client = new StreamClient(server_address);
        auto start_func = std::bind(&StreamClient::StartProcessors, client, distplan[i]);
        results.push_back(std::async(std::launch::async, start_func));
        clients.push_back(client);
    }

    // Block and wait for all nodes to finish execution
    ScopeTime scopetime;
    uint64_t res_count = 0, network_phase1 = 0, network_phase2 = 0;
    for (auto &res : results) {
        auto msg = res.get();
        res_count += msg.result_counts();
        network_phase1 += msg.network_phase1();
        network_phase2 += msg.network_phase2();
    }
    double run_time = scopetime.LogTime();
    double throughout = res_count / (1000000) / run_time;
    uint64_t network = network_phase1 + network_phase2;
    log_debug(
        "Distributed hash join result count : %ld, Shuffle network transmission : %ld bytes, Fetch network transmission : %ld, Total network transmission : %ld bytes(%.2lf "
        "kb, %.2lf mb), Execution time : %.3lf s, Throughput : %.2lf M/s",
        res_count, network_phase1, network_phase2, network, network / 1024.0, network / (1024.0 * 1024), run_time,
        throughout);
}

pair<TableReaderRouterType, TableReaderRouterType> DistServer::getDistType(HJType hj_type) {
    int enable_ldsketch = stoi(Config::g_conf["enable_ld_sketch"]);
    TableReaderRouterType small_type, big_type;
    switch (hj_type) {
        case HJType::BASE:
            small_type = TableReaderRouterType::TableReaderInfo_RouterType_HASH;
            big_type = TableReaderRouterType::TableReaderInfo_RouterType_HASH;
            break;
        case HJType::PNR:
            small_type = TableReaderRouterType::TableReaderInfo_RouterType_HASH;
            big_type = TableReaderRouterType::TableReaderInfo_RouterType_AVERAGE;
            break;
        case HJType::PRPD:
            small_type = TableReaderRouterType::TableReaderInfo_RouterType_HASH;
            big_type = TableReaderRouterType::TableReaderInfo_RouterType_NATIVE;
            break;
        case HJType::BNR:
            small_type = TableReaderRouterType::TableReaderInfo_RouterType_HASH;
            if (enable_ldsketch == 0) {
                big_type = TableReaderRouterType::TableReaderInfo_RouterType_BALANCE;
            } else {
                big_type = TableReaderRouterType::TableReaderInfo_RouterType_LDSKETCH;
            }
            break;
        case HJType::FLOWJOIN:
            small_type = TableReaderRouterType::TableReaderInfo_RouterType_FLOWSMALL;
            big_type = TableReaderRouterType::TableReaderInfo_RouterType_FLOWBIG;
            break;
        default:
            assert(false);
            break;
    }
    return {small_type, big_type};
}

vector<ProcessorsInfo> DistServer::generateDistPlan(HJType hj_type) {
    int node_nums = stoi(Config::g_conf["node_nums"]);
    vector<string> small_files;
    vector<string> big_files;
    vector<string> node_server_addrs;
    for (int i = 0; i < node_nums; ++i) {
        small_files.push_back(m_data_manager->GetFileName(i, true));
        big_files.push_back(m_data_manager->GetFileName(i, false));
        node_server_addrs.push_back(Config::g_conf["node" + to_string(i)]);
    }

    auto pair_type = this->getDistType(hj_type);
    auto small_type = pair_type.first;
    auto big_type = pair_type.second;

    // Set tablereader and hashjoiner
    vector<TableReaderInfo> small_trs(node_nums);
    vector<TableReaderInfo> big_trs(node_nums);
    vector<HashJoinerInfo> hjs(node_nums);
    int tr_id = 0, global_stream_id = 0;
    for (int i = 0; i < node_nums; ++i) {
        auto &small_tr = small_trs[i];
        small_tr.set_id(tr_id++);
        small_tr.set_file_name(small_files[i]);
        small_tr.set_router_type(small_type);
        small_tr.set_tr_type(stream::TableReaderInfo_TableReaderType::TableReaderInfo_TableReaderType_SMALL);

        auto output_infos = small_tr.mutable_output_infos();
        for (int j = 0; j < node_nums; ++j) {
            StreamInfo info;
            int stream_id = global_stream_id++;
            stream::StreamInfo_StreamType type = i == j ? stream::StreamInfo_StreamType::StreamInfo_StreamType_LOCAL
                                                        : stream::StreamInfo_StreamType::StreamInfo_StreamType_REMOTE;
            info.set_stream_id(stream_id);
            info.set_type(type);
            info.set_server_address(node_server_addrs[j]);
            output_infos->Add(std::move(info));

            auto &hj = hjs[j];
            auto hj_info = hj.add_small_infos();
            hj_info->set_stream_id(stream_id);
            hj_info->set_type(type);
        }
    }
    for (int i = 0; i < node_nums; ++i) {
        auto &big_tr = big_trs[i];
        big_tr.set_id(tr_id++);
        big_tr.set_file_name(big_files[i]);
        big_tr.set_router_type(big_type);
        big_tr.set_tr_type(stream::TableReaderInfo_TableReaderType::TableReaderInfo_TableReaderType_BIG);

        auto output_infos = big_tr.mutable_output_infos();
        for (int j = 0; j < node_nums; ++j) {
            StreamInfo info;
            int stream_id = global_stream_id++;
            stream::StreamInfo_StreamType type = i == j ? stream::StreamInfo_StreamType::StreamInfo_StreamType_LOCAL
                                                        : stream::StreamInfo_StreamType::StreamInfo_StreamType_REMOTE;
            info.set_stream_id(stream_id);
            info.set_type(type);
            info.set_server_address(node_server_addrs[j]);
            output_infos->Add(std::move(info));

            auto &hj = hjs[j];
            auto hj_info = hj.add_big_infos();
            hj_info->set_stream_id(stream_id);
            hj_info->set_type(type);
        }
    }
    for (int i = 0; i < node_nums; ++i) {
        hjs[i].set_type(stream::HashJoinerInfo_HJType(hj_type));
        auto bd_infos = hjs[i].mutable_bd_infos();
        StreamInfo info;
        info.set_stream_id(global_stream_id++);
        info.set_type(stream::StreamInfo_StreamType::StreamInfo_StreamType_LOCAL);
        bd_infos->Add(std::move(info));

        for (int j = i + 1; j < node_nums; ++j) {
            auto &peer_hj = hjs[j];
            int stream_id = global_stream_id++;

            StreamInfo info1;
            info1.set_stream_id(stream_id);
            info1.set_type(stream::StreamInfo_StreamType::StreamInfo_StreamType_REMOTE);
            info1.set_server_address(node_server_addrs[j]);
            bd_infos->Add(std::move(info1));

            StreamInfo info2;
            info2.set_stream_id(stream_id);
            info2.set_type(stream::StreamInfo_StreamType::StreamInfo_StreamType_REMOTE);
            info2.set_server_address(node_server_addrs[i]);
            peer_hj.add_bd_infos()->CopyFrom(info2);
        }
    }
    if (hj_type == HJType::FLOWJOIN) {
        for (int i = 0; i < node_nums; ++i) {
            hjs[i].set_type(stream::HashJoinerInfo_HJType(hj_type));
            auto bd_infos = hjs[i].mutable_bd_infos();
            StreamInfo info;
            info.set_stream_id(global_stream_id++);
            info.set_type(stream::StreamInfo_StreamType::StreamInfo_StreamType_LOCAL);
            bd_infos->Add(std::move(info));

            for (int j = i + 1; j < node_nums; ++j) {
                auto &peer_hj = hjs[j];
                int stream_id = global_stream_id++;

                StreamInfo info1;
                info1.set_stream_id(stream_id);
                info1.set_type(stream::StreamInfo_StreamType::StreamInfo_StreamType_REMOTE);
                info1.set_server_address(node_server_addrs[j]);
                bd_infos->Add(std::move(info1));

                StreamInfo info2;
                info2.set_stream_id(stream_id);
                info2.set_type(stream::StreamInfo_StreamType::StreamInfo_StreamType_REMOTE);
                info2.set_server_address(node_server_addrs[i]);
                peer_hj.add_bd_infos()->CopyFrom(info2);
            }
        }
    }
    for (int i = 0; i < node_nums; ++i) {
        if (hj_type == HJType::FLOWJOIN) {
            assert(hjs[i].bd_infos_size() == node_nums * 2);
        } else {
            assert(hjs[i].bd_infos_size() == node_nums);
        }
    }

    // Calculate SFR
    int x = sqrt(node_nums);
    while (node_nums % x != 0) {
        x--;
    }
    int rows = x;
    int cols = node_nums / x;
    stream::SFRInfo sfr_info;
    sfr_info.set_n(node_nums);
    sfr_info.set_rows(rows);
    sfr_info.set_cols(cols);
    log_debug("计算节点数量 : %d,  SFR分区 : %d * %d", node_nums, rows, cols);

    // Organize ProcessorsInfo
    vector<ProcessorsInfo> processor_infos(node_nums);
    for (int i = 0; i < node_nums; ++i) {
        processor_infos[i].mutable_sfr_info()->CopyFrom(sfr_info);
        processor_infos[i].add_tablereader_infos()->CopyFrom(small_trs[i]);
        processor_infos[i].add_tablereader_infos()->CopyFrom(big_trs[i]);
        processor_infos[i].add_hashjoiner_infos()->CopyFrom(hjs[i]);
    }
    return processor_infos;
}

pair<vector<TableReader *>, HashJoiner *> DistServer::SetupProcessors(const ProcessorsInfo *processors_info,
                                                                      StreamServiceImpl *service) {
    auto hashjoiners_infos = processors_info->hashjoiner_infos();
    assert(hashjoiners_infos.size() == 1);
    auto hjer = new HashJoiner(hashjoiners_infos[0], service);

    auto tablereader_infos = processors_info->tablereader_infos();
    log_debug("tablereader数量 : %ld", tablereader_infos.size());
    vector<TableReader *> trs;
    for (auto &info : tablereader_infos) {
        TableReader *tr = new TableReader(info);
        trs.push_back(tr);
    }

    // Special settings for Flow-Join
    if (hjer->GetType() == HJType::FLOWJOIN) {
        assert(processors_info->sfr_info().n() > 0);
        SFRhelper sfr_helper(processors_info->sfr_info().n(), processors_info->sfr_info().rows(),
                             processors_info->sfr_info().cols());
        hjer->SetSFRHelper(sfr_helper);
        for (auto tr : trs) {
            if (tr->GetType() == TableReaderType::BIG) {
                hjer->AddBigTableReader(tr);
                tr->SetSFRHelper(sfr_helper);
                tr->SetHashjoiner(hjer);
            }
        }
    }

    // Deal with local stream
    unordered_map<int, OutStream *> outstreams;
    for (auto tr : trs) {
        for (auto stream : tr->GetOutStreams()) {
            outstreams[stream->GetID()] = stream;
        }
    }
    for (auto instream : hjer->GetInstreams()) {
        if (instream->GetType() == StreamType::LOCAL) {
            assert(outstreams[instream->GetID()] != nullptr);
            outstreams[instream->GetID()]->SetMQ(instream->GetMQ(), instream->GetSkewMQ());
        }
    }
    for (auto tr : trs) {
        for (auto stream : tr->GetOutStreams()) {
            if (stream->GetType() == StreamType::LOCAL) {
                stream->AssertMQ();
            }
        }
    }
    // for (auto bdstream : hjer->GetSmallBDStreams()) {
    //     if (bdstream->GetType() == StreamType::LOCAL) {
    //         bdstream->SetBDStream(bdstream);
    //     }
    // }
    // for (auto bdstream : hjer->GetBigBDStreams()) {
    //     if (bdstream->GetType() == StreamType::LOCAL) {
    //         bdstream->SetBDStream(bdstream);
    //     }
    // }

    return {trs, hjer};
}