#pragma once

#include "comm/basic_mailbox.hpp"

namespace xyz {

class SchedulerMailbox : public BasicMailbox {
public:
  SchedulerMailbox(Node scheduler_node, int num_workers);
  ~SchedulerMailbox();
  virtual void Start() override;

private:
  void UpdateHeartbeat(int node_id);
  virtual void HandleBarrierMsg() override;
  virtual void HandleRegisterMsg(Message *msg, Node &recovery_node) override;
  virtual void UpdateID(Message *msg, std::unordered_set<int> *deadnodes_set,
                        Node &recovery_node) override;
  virtual void Receiving() override;
};
} // namespace xyz