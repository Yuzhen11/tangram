#pragma once

#include "base/message.hpp"
#include "core/intermediate/abstract_intermediate_store.hpp"
#include "comm/abstract_sender.hpp"

namespace xyz {

class IntermediateStore : public AbstractIntermediateStore {
 public:
  IntermediateStore(std::shared_ptr<AbstractSender> sender)
      :sender_(sender) {}

  virtual void Add(Message msg) override {
    sender_->Send(std::move(msg));
  }
 private:
  std::shared_ptr<AbstractSender> sender_;
};

}  // namespace xyz

