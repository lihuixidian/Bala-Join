#ifndef HASHJOINER_H_
#define HASHJOINER_H_

#include "../base/BDStream.hpp"
#include "Input.hpp"
#include "Output.hpp"

class StreamServiceImpl;
class TableReader;

enum HJType { BASE, PNR, PRPD, BNR, FLOWJOIN };

class HashJoiner {
   public:
    HashJoiner(const HashJoinerInfo& hashjoiner_info, StreamServiceImpl* stream_service);
    ~HashJoiner();

   public:
    uint64_t Run();
    std::pair<uint64_t, uint64_t> CalRecieveBytes();
    std::vector<InStream*> GetInstreams();
    std::vector<BDStream*> GetSmallBDStreams() { return m_small_bdstreams; }
    std::vector<BDStream*> GetBigBDStreams() { return m_big_bdstreams; }
    HJType GetType() { return m_type; }
    void AddBigTableReader(TableReader* tr) { m_local_big_tr.push_back(tr); }
    void SetSFRHelper(const SFRhelper& sfr) { m_sfr_helper = new SFRhelper(sfr); }
    void AddBigIntersectTuple(const Tuple& tuple) { m_big_intersect_tuples[tuple.key()].push_back(tuple); }

   public:
    uint64_t runFlowJoin();

   private:
    HJType m_type;                          // hashjoin type
    Input* m_big_input;                     // big table input
    Input* m_small_input;                   // small table input
    std::vector<BDStream*> m_small_bdstreams; // fetch streams
    std::vector<BDStream*> m_big_bdstreams;   // fetch streams
    Output* m_output;                       // output
    StreamServiceImpl* m_stream_service;    // owning service

    std::unordered_map<std::string, std::vector<Tuple>> m_hashmap; // for building the build-table
    std::unordered_set<std::string> m_global_small_skews;         // for Flow-Join
    
    SFRhelper* m_sfr_helper;               // used for flowjoin
    std::vector<TableReader*> m_local_big_tr; // local big TableReader (for FlowJoin)
    std::unordered_map<std::string, std::vector<Tuple>> m_big_intersect_tuples;
};

#endif