#pragma once

#include "base/message.hpp"

namespace xyz {

class AbstractIntermediateStore {
 public:
  virtual ~AbstractIntermediateStore() {}
  virtual void Add(Message msg) = 0;
};

}  // namespace xyz
