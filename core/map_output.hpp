#pragma once

#include "core/abstract_map_output.hpp"

#include <vector>

namespace xyz {

template<typename KeyT, typename MsgT>
class MapOutput: public TypedMapOutput<KeyT, MsgT> {
 public:
  virtual ~MapOutput() {}
  virtual void Add(std::pair<KeyT, MsgT> msg) override {
    buffer_.push_back(std::move(msg));
  }

  virtual SArrayBinStream Serialize() override {
    SArrayBinStream bin;
    // Now we push into binstream one by one.
    // May consider to use SArray as the underlying storage for SArrayMapOuput, so
    // that no need to serialize at all when the KeyT and MsgT are both trivially
    // copyable.
    for (auto& p : buffer_) {
      bin << p.first << p.second;
    }
    return bin;
  }

  // For debug usage
  std::vector<std::pair<KeyT, MsgT>> Get() const {
    return buffer_;
  }
 private:
  std::vector<std::pair<KeyT, MsgT>> buffer_;
};

}  // namespace

