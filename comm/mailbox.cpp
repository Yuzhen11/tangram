#include "comm/mailbox.hpp"

#include "base/third_party/network_utils.h"

namespace xyz {


Mailbox::Mailbox(bool is_scheduler, SchedulerInfo scheduler_node)
    : is_scheduler_(is_scheduler), scheduler_node_(scheduler_node){
}

Mailbox::~Mailbox() {
}

void Mailbox::Bind(const Node& node, int max_retry) {
  // Add max_retry
  receiver_ = zmq_socket(context_, ZMQ_ROUTER);
  CHECK(receiver_ != nullptr) << "create receiver socket failed: " << zmq_strerror(errno);
  std::string address = "tcp://*:" + std::to_string(node.port);
  if (zmq_bind(receiver_, address.c_str()) != 0) {
    LOG(FATAL) << "bind to " + address + " failed: " << zmq_strerror(errno);
  }
}

void Mailbox::Connect(const Node& node) {
  CHECK_NE(node.id, node.kEmpty);
  CHECK_NE(node.port, node.kEmpty);
  auto it = senders_.find(node.id);
  if (it != senders_.end()) {
    zmq_close(it->second);
  }
  void* sender = zmq_socket(context_, ZMQ_DEALER);
  CHECK(sender != nullptr) << zmq_strerror(errno);
  std::string my_id = "ps" + std::to_string(node_.id);
  zmq_setsockopt(sender, ZMQ_IDENTITY, my_id.data(), my_id.size());
  std::string addr = "tcp://" + node.hostname + ":" + std::to_string(node.port);
  if (zmq_connect(sender, addr.c_str()) != 0) {
    LOG(FATAL) << "connect to " + addr + " failed: " << zmq_strerror(errno);
  }
  senders_[node.id] = sender;
}

void Mailbox::RegisterQueue(uint32_t queue_id, ThreadsafeQueue<Message>* const queue) {
}

void Mailbox::DeregisterQueue(uint32_t queue_id) {
}

int Mailbox::Send(const Message& msg) {
}

int Mailbox::Recv(Message* msg) {
}

void Mailbox::Barrier() {
}

void Mailbox::Start() {
  if (is_scheduler_) {
    my_node_ = scheduler_node_;
  } else {
    std::string interface;
    std::string ip;
    third_party::GetAvailableInterfaceAndIP(&interface, &ip);
    CHECK(!interface.empty()) << "failed to get the interface";
    int port = third_party::GetAvailablePort();
    CHECK(!ip.empty()) << "failed to get ip";
    CHECK(port) << "failed to get a port";
    my_node_.hostname = ip;
    my_node_.port = port;
    // cannot determine my id now, the scheduler will assign it later
    my_node_.id = Node::kEmpty;
  }

  // bind
  my_node_.port = Bind(my_node_, is_scheduler ? 0:40);
  VLOG(1) << "Bind to " << my_node_.DebugString();
  CHECK_NE(my_node_.port, -1) << "bind failed";

  // connect to scheduler
  Connect(scheduler_node_);

  receiver_thread_ = std::thread(&Mailbox::Receiving, this);

  if (!is_scheduler_) {
    // let the scheduler know myself
    Messsage msg;
    msg.meta.recver = ;
  }

  while (!ready_) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  if (!is_scheduler_) {
    heartbeat_thread_ = std::thread(&Mailbox::Heartbeat, this);
  }
}

void Mailbox::Stop() {
  // TODO
  receiver_thread_.join();
  if (!is_scheduler_) heartbeat_thread_.join();
}

void Mailbox::Receiving() {
}

void Mailbox::Heartbeat() {
}

}  // namespace xyz
