#ifndef INPUT_H_
#define INPUT_H_

#include <unordered_set>
#include <vector>

#include "../base/InStream.hpp"

using namespace std;

class StreamServiceImpl;

class Input {
   public:
    Input(const vector<StreamInfo>& stream_infos, StreamServiceImpl* stream_service);
    ~Input();

    pair<vector<string>, BatchTuple> Next();
    vector<InStream*> GetStreams() { return m_streams_vct; }

private:
    unordered_set<InStream *> m_streams; // Multiple input streams
    vector<InStream *> m_streams_vct;    // Multiple input streams
    StreamServiceImpl *m_stream_service; // The service it belongs to
};

#endif