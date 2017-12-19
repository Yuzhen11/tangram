#pragma once

#include <utility>

namespace xyz {

class AbstractMapOutput {
 public:
  virtual ~AbstractMapOutput() {}
};

template<typename KeyT, typename MsgT>
class TypedMapOutput : public AbstractMapOutput {
 public:
  virtual void Add(std::pair<KeyT, MsgT> msg) = 0;
};

}  // namespace

