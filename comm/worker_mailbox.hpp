#pragma once

#include "comm/basic_mailbox.hpp"

namespace xyz {

class WorkerMailbox : public BasicMailbox {
public:
  WorkerMailbox(Node scheduler_node, int num_workers);
  ~WorkerMailbox();

  virtual void Start() override;
  void Stop() override;

private:
  // heartbeat
  std::thread heartbeat_thread_;

  virtual void Heartbeat();
  virtual void HandleBarrierMsg() override;
  virtual void HandleRegisterMsg(Message *msg, Node &recovery_node) override;
  virtual void UpdateID(Message *msg, std::unordered_set<int> *deadnodes_set,
                        Node &recovery_node) override;
  virtual void Receiving() override;
};
} // namespace xyz