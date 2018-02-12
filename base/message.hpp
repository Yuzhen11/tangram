#pragma once

#include <cinttypes>
#include <sstream>
#include <vector>

#include "base/third_party/sarray.h"
#include "base/node.hpp"

namespace xyz {
class SArrayBinStream;

enum class Flag : char {
  kMailboxControl,
  kActorExit,
  kOthers
};
static const char* FlagName[] = {
  "kMailboxControl",
  "kActorExit",
  "kOthers"
};

struct Meta {
  int sender;
  int recver;
  Flag flag;

  std::string DebugString() const {
    std::stringstream ss;
    ss << "Meta: { ";
    ss << "sender: " << sender;
    ss << ", recver: " << recver;
    ss << ", flag: " << FlagName[static_cast<int>(flag)];
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
