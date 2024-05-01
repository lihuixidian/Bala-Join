#include "config.hpp"

#include <fstream>
#include <iostream>
#include <string>

#include "debug.hpp"
#include "log.hpp"

std::unordered_map<std::string, std::string> Config::g_conf;

void Config::Initialize(int argc, char **argv) {
    assert(argc >= 2);
    std::string file = argv[1];
    std::ifstream fin(file);
    std::string line;
    while (fin >> line) {
        int p = line.find("=");
        if (line[0] == '#' || p == std::string::npos) continue;
        std::string key = line.substr(0, p);
        std::string value = line.substr(p + 1, line.size() - p - 1);
        g_conf.insert({key, value});
    }
    for (auto &it : g_conf) {
        log_info("config [%s = %s]", it.first.c_str(), it.second.c_str());
    }
}
