#pragma once

#include <utility>
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
};

}  // namespace xyz
