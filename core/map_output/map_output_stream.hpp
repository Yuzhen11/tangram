#pragma once

#include <utility>
#include <vector>
#include <algorithm>
#include "base/sarray_binstream.hpp"

namespace xyz {

class AbstractMapOutputStream {
 public:
  virtual ~AbstractMapOutputStream() {}

  virtual SArrayBinStream Serialize() = 0;
};


template<typename KeyT, typename MsgT>
class MapOutputStream : public AbstractMapOutputStream {
 public:
  MapOutputStream() = default;

  void Add(std::pair<KeyT, MsgT> msg) {
    buffer_.push_back(std::move(msg));
  }

  virtual SArrayBinStream Serialize() override {
    return SerializeOneBuffer(buffer_);
  }

  static SArrayBinStream SerializeOneBuffer(const std::vector<std::pair<KeyT, MsgT>>& buffer) {
    SArrayBinStream bin;
    for (auto& p : buffer) {
      bin << p.first << p.second;
    }
    return bin;
  }

  void Combine(const std::function<void(MsgT*, const MsgT&)>& combine_func) {
    std::sort(buffer_.begin(), buffer_.end(), 
      [](const std::pair<KeyT, MsgT>& p1, const std::pair<KeyT, MsgT>& p2) { return p1.first < p2.first; });
    CombineOneBuffer(buffer_, combine_func);
  }

  static void CombineOneBuffer(std::vector<std::pair<KeyT, MsgT>>& buffer, 
          const std::function<void(MsgT*, const MsgT&)>& combine) {
    int l = 0;
    for (int r = 1; r < buffer.size(); ++ r) {
      if (buffer[l].first == buffer[r].first) {
        combine(&(buffer[l].second), buffer[r].second);
      } else {
        l += 1;
        if (l != r) {
          buffer[l] = buffer[r];
        }
      }
    }
    if (!buffer.empty()) {
      buffer.resize(l+1);
    }
  }

  // For test use only.
  const std::vector<std::pair<KeyT, MsgT>>& GetBuffer() const { 
    return buffer_;
  }
 private:
  std::vector<std::pair<KeyT, MsgT>> buffer_;
};

}  // namespace xyz

