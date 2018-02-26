#pragma once

#include "base/actor.hpp"
#include "comm/abstract_mailbox.hpp"
#include "comm/abstract_sender.hpp"

namespace xyz {

class Sender : public AbstractSender, public Actor {
 public:
  Sender(int qid, AbstractMailbox* mailbox) 
      : Actor(qid), mailbox_(mailbox) {
    Start();
  }
  ~Sender() {
    Stop();
  }

  virtual void Send(Message msg) override;
  virtual void Process(Message msg) override;

private:
  AbstractMailbox *mailbox_;
};

} // namespace xyz
