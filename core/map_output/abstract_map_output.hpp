#pragma once

#include <utility>
#include <vector>
#include "base/sarray_binstream.hpp"

namespace xyz {

class AbstractMapOutput {
 public:
  virtual ~AbstractMapOutput() {}
  virtual SArrayBinStream Serialize() = 0;
};

template<typename KeyT, typename MsgT>
class TypedMapOutput : public AbstractMapOutput {
 public:
  virtual void Add(std::pair<KeyT, MsgT> msg) = 0;
  virtual void Add(std::vector<std::pair<KeyT, MsgT>> msgs) = 0;
};

}  // namespace xyz
