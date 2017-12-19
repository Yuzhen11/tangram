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

  // For debug usage
  std::vector<std::pair<KeyT, MsgT>> Get() const {
    return buffer_;
  }
 private:
  std::vector<std::pair<KeyT, MsgT>> buffer_;
};

}  // namespace

