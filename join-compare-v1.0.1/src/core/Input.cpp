#include "Input.hpp"
#include "../serviceImpl/StreamServiceImpl.hpp"
#include "../comm/log.hpp"

Input::Input(const vector<StreamInfo>& stream_infos, StreamServiceImpl* stream_service) {
    m_stream_service = stream_service;
    for (auto& info : stream_infos) {
        auto stream = new InStream(info);
        m_streams.insert(stream);
        m_streams_vct.push_back(stream);
        m_stream_service->RegisterStream(stream);
    }
}

Input::~Input() {
    for (auto stream : m_streams_vct) {
        m_stream_service->UnRegisterStream(stream);
        if (stream != nullptr) delete stream;
    }
}

pair<vector<string>, BatchTuple> Input::Next() {
    unordered_set<string> skew_keys;
    while (true) {
        if (m_streams.empty()) break;
        for (auto stream : m_streams) {
            while (stream->HasSkew()) {
                skew_keys.insert(stream->GetSkewKey());
            }
            if (stream->HasBatch()) {
                auto batch = stream->Get();
                if (batch.empty()) {
                    m_streams.erase(stream);
                    break;
                }
                vector<string> vct(skew_keys.begin(), skew_keys.end());
                return {vct, batch};
            }
        }
    }
    vector<string> vct(skew_keys.begin(), skew_keys.end());
    return {vct, EmptyBatch()};
}