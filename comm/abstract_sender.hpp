#pragma once

#include "base/message.hpp"

namespace xyz {

class AbstractSender {
 public:
  virtual ~AbstractSender() {}
  virtual void Send(Message msg) = 0;
};

}  // namespace xyz
