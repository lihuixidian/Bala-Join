#ifndef HISTOGRAM_H_
#define HISTOGRAM_H_

#include <unordered_map>
#include <unordered_set>
#include <vector>

using namespace std;

class Histogram {
   public:
    Histogram(string table_file);

    // Sample a key, return true if the key is skewed
    bool SampleKey(const string& key);
    void debug();

   private:
    void Adjust(int index);  // Adjust the index position to maintain descending order by count

   private:
    int m_sample_capacity;          // Approximate histogram capacity
    double m_skew_threshold_ratio;  // Skew threshold
    int m_sample_count;             // Number of samples

    bool m_dynamic_detect;                      // Dynamic detection
    unordered_map<string, int> m_statistic_skew;  // Static skew data, used when dynamic detection is not enabled

    vector<pair<string, int>> m_counts_pair;   // Array of (key, count) pairs, each key corresponds to a count, sorted in descending order by count
    unordered_map<string, int> m_counts;       // Map from key to index, where index is the position in the m_counts_pair array
    unordered_map<string, int> m_init_counts;  // Initial count when a key is inserted

    vector<int> m_results;  // Sampling results
};

#endif