#pragma once

#include "core/map_output/abstract_map_output.hpp"

#include <vector>

#include "glog/logging.h"

namespace xyz {

template<typename KeyT, typename MsgT>
class MapOutput: public TypedMapOutput<KeyT, MsgT> {
 public:
  virtual ~MapOutput() {}
  virtual void Add(std::pair<KeyT, MsgT> msg) override {
    buffer_.push_back(std::move(msg));
  }
  virtual void Add(std::vector<std::pair<KeyT, MsgT>> msgs) override {
    buffer_.insert(buffer_.end(), msgs.begin(), msgs.end());
  }

  virtual void Combine() override {
    CHECK(false) << "Not implemented";
  }

  virtual std::vector<SArrayBinStream> Serialize() override {
    CHECK(false) << "Not implemented";
  }

  // For debug usage
  std::vector<std::pair<KeyT, MsgT>> Get() const {
    return buffer_;
  }
 private:
  std::vector<std::pair<KeyT, MsgT>> buffer_;
};

}  // namespace

