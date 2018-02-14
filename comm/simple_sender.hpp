#include "comm/abstract_sender.hpp"

namespace xyz {

class SimpleSender : public AbstractSender {
 public:
  virtual void Send(Message msg) override {
    msgs.Push(std::move(msg));
  }
  Message Get() {
    Message msg;
    msgs.WaitAndPop(&msg);
    return msg;
  }
  ThreadsafeQueue<Message> msgs;
};

}  // namespace xyz

