#pragma once

#include <atomic>
#include <future>
#include <map>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <zmq.h>

#include "base/message.hpp"
#include "base/node.hpp"
#include "base/sarray_binstream.hpp"
#include "base/third_party/network_utils.h"
#include "base/threadsafe_queue.hpp"
#include "comm/abstract_mailbox.hpp"
#include "glog/logging.h"

namespace xyz {

enum class MailboxFlag : char { kExit, kBarrier, kRegister, kHeartbeat };
static const char *MailboxFlagName[] = {"kExit", "kBarrier", "kRegister",
                                        "kHeartbeat"};

struct Control {
  MailboxFlag flag;
  Node node;
  bool is_recovery = false;

  friend SArrayBinStream &operator<<(xyz::SArrayBinStream &stream,
                                     const Control &ctrl);
  friend SArrayBinStream &operator>>(xyz::SArrayBinStream &stream,
                                     Control &ctrl);
};

inline void FreeData(void *data, void *hint) {
  if (hint == NULL) {
    delete[] static_cast<char *>(data);
  } else {
    delete static_cast<third_party::SArray<char> *>(hint);
  }
}

class BasicMailbox : public AbstractMailbox {
public:
  BasicMailbox(Node scheduler_node, int num_workers);
  ~BasicMailbox();

  void RegisterQueue(uint32_t queue_id, ThreadsafeQueue<Message> *const queue);
  void DeregisterQueue(uint32_t queue_id);

  virtual void Start() = 0;

  virtual void Stop();

  // return # of bytes sended
  virtual int Send(const Message &msg) override;

  int Recv(Message *msg);

  void Barrier();

  // For testing only
  void BindAndConnect();

  void CloseSockets();

  void Connect(const Node &node);

  const Node &my_node() const;

  std::vector<Node> GetNodes();

protected:
  void Bind(const Node &node, int max_retry);

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
  const std::vector<int> GetNodeIDs();

  std::map<uint32_t, ThreadsafeQueue<Message> *const> queue_map_;

  // Handle different msgs
  virtual void HandleBarrierMsg() = 0;
  virtual void HandleRegisterMsg(Message *msg, Node &recovery_node) = 0;
  virtual void UpdateID(Message *msg, std::unordered_set<int> *deadnodes_set,
                        Node &recovery_node) = 0;

  // receiver
  std::thread receiver_thread_;
  virtual void Receiving() = 0;

  // socket
  void *context_ = nullptr;
  std::unordered_map<uint32_t, void *> senders_;
  void *receiver_ = nullptr;
  std::mutex mu_;

  // only used by scheduler (ps-lite put these in postoffice)
  std::mutex heartbeat_mu_;
  std::unordered_map<int, time_t> heartbeats_;
  // make it get from outside in the future
  int heartbeat_timeout_ = 60;
  std::vector<int> GetDeadNodes(int timeout = 60);

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

} // namespace xyz
