#pragma once

#include "comm/basic_mailbox.hpp"

namespace xyz {

class SchedulerMailbox : public BasicMailbox {
public:
  SchedulerMailbox(Node scheduler_node, int num_workers);
  ~SchedulerMailbox();
  virtual void Start() override;

private:
  std::mutex heartbeat_mu_;
  std::unordered_map<int, time_t> heartbeats_; // heartbeats from workers
  // in seconds
  const int kHeartbeatTimeout = 1;
  const int kHeartbeatCheckInterval = 1;
  std::vector<int> GetDeadNodes(int timeout = 60);
  void CheckHeartbeat(int time_out);
  void UpdateHeartbeat(int node_id);

  virtual void HandleBarrierMsg() override;
  virtual void HandleRegisterMsg(Message *msg, Node &recovery_node) override;
  virtual void Receiving() override;
  const std::vector<int> GetNodeIDs();
  void UpdateID(Message *msg, std::unordered_set<int> *deadnodes_set,
                Node &recovery_node);

  int num_workers_;
};
} // namespace xyz
