#pragma once

#include <cinttypes>
#include <sstream>

#include "base/third_party/sarray.h"
#include "base/node.hpp"

namespace xyz {

struct Control {};

enum class Flag : char {kExit, kBarrier, kRegister, kHeartbeat};
static const char* FlagName[] = {"kExit", "kRegister", "kBarrier", "kHeartbeat"};

struct Meta {
  int sender;
  int recver;
  int partition_id;
  int collection_id;
  Flag flag;  // { kExit, kBarrier, kHeartbeat};
  Node node;
  int timestamp;

  std::string DebugString() const {
    std::stringstream ss;
    ss << "Meta: { ";
    ss << "sender: " << sender;
    ss << ", recver: " << recver;
    ss << ", collection_id: " << collection_id;
    ss << ", partition_id: " << partition_id;
    ss << ", flag: " << FlagName[static_cast<int>(flag)];
    ss << ", timestamp: " << timestamp;
    ss << "}";
    return ss.str();
  }
};

struct Message {
  Meta meta;
  std::vector<third_party::SArray<char>> data;

  template <typename V>
  void AddData(const third_party::SArray<V>& val) {
    data.push_back(third_party::SArray<char>(val));
  }

  std::string DebugString() const {
    std::stringstream ss;
    ss << meta.DebugString();
    if (data.size()) {
      ss << " Body:";
      for (const auto& d : data)
        ss << " data_size=" << d.size();
    }
    return ss.str();
  }
};

}  // namespace xyz
