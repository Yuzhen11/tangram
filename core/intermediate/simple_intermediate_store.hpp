#pragma once

#include "base/message.hpp"
#include "core/intermediate/abstract_intermediate_store.hpp"

namespace xyz {

class SimpleIntermediateStore : public AbstractIntermediateStore {
 public:
  virtual void Add(Message msg) override {
    msgs.push_back(std::move(msg));
  }

  std::vector<Message> Get() const {
    return msgs;
  }
 private:
  std::vector<Message> msgs;
};

}  // namespace xyz
