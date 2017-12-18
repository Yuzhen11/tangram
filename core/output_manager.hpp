#pragma once

#include "abstract_output_manager.hpp"

#include <vector>

namespace xyz {

template<typename KeyT, typename MsgT>
class OutputManager: public AbstractOutputManager {
 public:
  virtual ~OutputManager() {}
  void Add(std::pair<KeyT, MsgT> msg) {
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

