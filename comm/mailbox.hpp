#pragma once

#include <atomic>
#include <map>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <zmq.h>

#include "base/threadsafe_queue.hpp"
#include "base/node.hpp"
#include "base/message.hpp"
// #include "comm/resender.hpp"
#include "base/third_party/network_utils.h"
#include "base/sarray_binstream.hpp"
#include "glog/logging.h"

namespace xyz {


class Mailbox {
 public:
  Mailbox(bool is_scheduler, Node scheduler_node, int num_workers);
  ~Mailbox();

  void RegisterQueue(uint32_t queue_id, ThreadsafeQueue<Message>* const queue);
  void DeregisterQueue(uint32_t queue_id);

  // return # of bytes sended
  int Send(const Message& msg);
  int Recv(Message* msg);
  void Barrier();

  // For testing only
  void BindAndConnect();
  void StartReceiving();
  void StopReceiving();
  
  void CloseSockets();
  void Start();
  void Stop();
  void Connect(const Node& node);

  const Node& my_node() const {
    CHECK(ready_) << "call Start() first";
    return my_node_;
  }
 private:
  void Bind(const Node& node, int max_retry);
  

  bool is_scheduler_;
  // whether it is ready for sending
  std::atomic<bool> ready_{false};
  Node scheduler_node_;
  Node my_node_;

  // all worker nodes
  std::vector<Node> nodes_;
  int num_workers_;

  // node's address string (i.e. ip:port) -> node id
  // this map is updated when ip:port is received for the first time
  std::unordered_map<std::string, int> connected_nodes_;
  // maps the id of node which is added later to the id of node
// which is with the same ip:port and added first
std::unordered_map<int, int> shared_node_mapping_;
const std::vector<int> GetNodeIDs() const;

  std::map<uint32_t, ThreadsafeQueue<Message>* const> queue_map_;

  // Handle different msgs
  void HandleBarrierMsg();
  void HandleRegisterMsg(Message* msg, Node& recovery_node);
  void HandleRegisterMsgAtScheduler(Node& recovery_node);
  void HandleHeartbeat(int node_id);

  void UpdateID(Message* msg, std::unordered_set<int>* deadnodes_set, Node& recovery_node);

  // receiver
  std::thread receiver_thread_;
  void Receiving();

  // socket
  void* context_ = nullptr;
  std::unordered_map<uint32_t, void*> senders_;
  void* receiver_ = nullptr;
  std::mutex mu_;

  // heartbeat
  std::thread heartbeat_thread_;
  void Heartbeat();
  void UpdateHeartbeat(int node_id, time_t t);
  // only used by scheduler (ps-lite put these in postoffice)
  std::mutex heartbeat_mu_;
  std::unordered_map<int, time_t> heartbeats_;
  // make it get from outside in the future
  int heartbeat_timeout_ = 60;
  std::vector<int> GetDeadNodes(int t = 60);
  // start time of the node
  time_t start_time_;

  // barrier
  bool barrier_finish_ = false;
  int barrier_count_ = 0;
  std::condition_variable barrier_cond_;

  // msg resender
  // Resender *resender_ = nullptr;
  std::atomic<int> timestamp_{0};
};

}  // namespace xyz
