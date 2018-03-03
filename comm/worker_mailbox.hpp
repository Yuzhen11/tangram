#pragma once

#include "comm/basic_mailbox.hpp"

namespace xyz {

class WorkerMailbox : public BasicMailbox {
public:
  WorkerMailbox(Node scheduler_node, int num_workers);
  ~WorkerMailbox();

  virtual void Start() override;
  // Just for test
  virtual void StopHeartbeat();

private:
  virtual void Heartbeat();
  virtual void HandleBarrierMsg() override;
  virtual void HandleRegisterMsg(Message *msg, Node &recovery_node) override;
  void UpdateID(Message *msg, Node &recovery_node);
  virtual void Receiving() override;
};
} // namespace xyz