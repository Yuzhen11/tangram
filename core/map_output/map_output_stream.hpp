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
  virtual void Combine() = 0;
  virtual void Append(std::shared_ptr<AbstractMapOutputStream> other) = 0;
  virtual void Clear() = 0;
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

  virtual void Append(std::shared_ptr<AbstractMapOutputStream> other) override {
    auto* p = static_cast<MapOutputStream<KeyT, MsgT>*>(other.get());
    const auto& other_buffer = p->GetBuffer();
    buffer_.insert(buffer_.end(), other_buffer.begin(), other_buffer.end());
  }

  virtual void Clear() override {
    buffer_.clear();
  }

  static SArrayBinStream SerializeOneBuffer(const std::vector<std::pair<KeyT, MsgT>>& buffer) {
    SArrayBinStream bin;
    for (auto& p : buffer) {
      bin << p.first << p.second;
    }
    return bin;
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

  using CombineFuncT = std::function<void(MsgT*, const MsgT&)>;
  void SetCombineFunc(CombineFuncT combine_func) {
    combine_func_ = std::move(combine_func);
  }

  virtual void Combine() override {
    if (!combine_func_) 
      return;
    // 1. sort
    std::sort(buffer_.begin(), buffer_.end(), 
      [](const std::pair<KeyT, MsgT>& p1, const std::pair<KeyT, MsgT>& p2) { return p1.first < p2.first; });
    // 2. combine
    CombineOneBuffer(buffer_, combine_func_);
  };

  // For test use only.
  const std::vector<std::pair<KeyT, MsgT>>& GetBuffer() const { 
    return buffer_;
  }
 private:
  std::vector<std::pair<KeyT, MsgT>> buffer_;

  CombineFuncT combine_func_;  // optional
};

}  // namespace xyz

