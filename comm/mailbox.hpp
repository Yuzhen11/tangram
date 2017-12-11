#pragma once

#include "base/threadsafe_queue.hpp"
#include "base/node.hpp"

#include "glog/logging.h"

namespace xyz {

class Mailbox {
 public:
  Mailbox(bool is_scheduler, Node scheduler_node);
  ~Mailbox();

  void RegisterQueue(uint32_t queue_id, ThreadsafeQueue<Message>* const queue);
  void DeregisterQueue(uint32_t queue_id);

  int Send(const Message& msg);
  int Recv(Message* msg);
  void Barrier();

  void Start();
  void Stop();

  const Node& my_node() const {
    CHECK(ready_) << "call Start() first";
    return my_node_;
  }
 private:
  int Bind(const Node& node, int max_retry);
  int Connect(const Node& node);

  bool is_scheduler_;
  SchedulerInfo scheduler_node_;

  std::map<uint32_t, ThreadsafeQueue<Message>* const> queue_map_;

  // receiver
  std::thread receiver_thread_;
  void Receiving();

  Node my_node_;

  // socket
  void* context_ = nullptr;
  std::unordered_map<uint32_t, void*> senders_;
  void* receiver_ = nullptr;
  std::mutex mu_;

  // heartbeat
  std::thread heartbeat_thread_;
  void Heartbeat();
};

}  // namespace xyz
