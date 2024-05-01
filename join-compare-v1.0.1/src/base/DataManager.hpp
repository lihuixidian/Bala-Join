#ifndef DATAMANAGER_H_
#define DATAMANAGER_H_

#include <random>
#include <string>
#include <vector>

#include "Headers.hpp"
using namespace std;

/*
    Data Management Class:
    1. skew_degree is the zipf factor for a single table, degree > 1
    2. File directory: resource/{node_nums}_{small_table_size}_{small_skew_degree}_{big_table_size}_{big_skew_degree}/{nodeID}/
*/
class DataManager {
   public:
    DataManager(int small_table_size, int big_table_size, double small_skew_degree, double big_skew_data,
                int node_nums);

    vector<Tuple> GetData(const string& file_name);
    string GetFileName(int nodeID, bool small);
    void CheckData();

   private:
    vector<Tuple> Generate(uint64_t size, double skew_degree);
    void WriteData();

   private:
    default_random_engine m_rand_seed;  // Random seed
    double m_big_skew_degree;           // Skewness degree of the big table
    double m_small_skew_degree;         // Skewness degree of the small table
    int m_small_table_size;             // Data size of the small table
    int m_big_table_size;               // Data size of the big table
    string m_dir;                       // Directory
    int m_node_nums;                    // Number of nodes to read
    vector<vector<Tuple>> m_data;       // Two tables data obtained {small table, big table}
};

#endif
