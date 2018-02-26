#pragma once

#include "base/message.hpp"

namespace xyz {

class AbstractMailbox {
public:
  virtual ~AbstractMailbox() = default;
  virtual int Send(const Message &msg) = 0;
};

} // namespace xyz
