#ifndef TABLEREADER_H_
#define TABLEREADER_H_

#include "Output.hpp"
#include "Router.hpp"

class HashJoiner;

enum TableReaderType { SMALL, BIG };

class TableReader {
   public:
    TableReader(const TableReaderInfo& tablereader_info);
    ~TableReader();

   public:
    void Run();
    void SetSFRHelper(SFRhelper& sfr) { m_sfr_helper = new SFRhelper(sfr); }
    void SetHashjoiner(HashJoiner* hjer) { m_local_hjer = hjer; }
    TableReaderType GetType() { return m_tr_type; }
    vector<OutStream*> GetOutStreams() { return m_output->GetStreams(); }
    void SetGlobalSmallSkews(unordered_set<string>* skews) {
        assert(m_tr_type == TableReaderType::BIG);
        m_global_small_skews = skews;
        m_condition.Notify();
    }

   private:
    void readData();
    void distributeDataForHash();
    void distributeDataForNative();
    void distributeDataForAverage();
    void distributeDataForBalance();
    void distributeDataForFlowSmall();
    void distributeDataForFlowBig();
    void distributeDataForLDSketch();

    private:
        int m_id;                  // tablereader id
        TableReaderType m_tr_type; // type of tablereader
        string m_file_name;        // file to read
        RouterType m_router_type;  // type of router
        vector<Tuple> m_data;      // data read
        Output *m_output;          // output operator
        SFRhelper *m_sfr_helper;   // used by flowjoin
        HashJoiner *m_local_hjer;  // local hashjoiner

        Condition m_condition;                       // wakes up when m_global_small_skews is set
        unordered_set<string> *m_global_small_skews; // global skew values for small
};

#endif