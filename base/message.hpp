#pragma once

#include <cinttypes>
#include <sstream>
#include <vector>

#include "base/third_party/sarray.h"
#include "base/node.hpp"

namespace xyz {
class SArrayBinStream;
enum class Flag : char {kExit, kBarrier, kRegister, kAdd, kGet, kHeartbeat, kFetch, kFetchReply};
static const char* FlagName[] = {"kExit", "kBarrier", "kRegister", "kAdd", "kGet", "kHeartbeat", "kFetch", "kFetchReply"};

struct Control {
	Flag flag;
    int partition_id;
    int collection_id;
    Node node;
    bool is_recovery = false;
    int timestamp;

	friend SArrayBinStream& operator<<(xyz::SArrayBinStream& stream, const Control& ctrl);
    friend SArrayBinStream& operator>>(xyz::SArrayBinStream& stream, Control& ctrl);
};

struct Meta {
  int sender;
  int recver;
  bool is_ctrl;

  std::string DebugString() const {
    std::stringstream ss;
    ss << "Meta: { ";
    ss << "sender: " << sender;
    ss << ", recver: " << recver;
    ss << ", is_ctrl: " << is_ctrl;
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
