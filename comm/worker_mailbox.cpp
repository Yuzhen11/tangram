#include "comm/worker_mailbox.hpp"

namespace xyz {

WorkerMailbox::WorkerMailbox(Node scheduler_node, int num_workers)
    : BasicMailbox(scheduler_node, num_workers) {}

WorkerMailbox::~WorkerMailbox() {}

void WorkerMailbox::Start() {
  // start zmq
  mu_.lock();
  if (context_ == nullptr) {
    context_ = zmq_ctx_new();
    CHECK(context_ != NULL) << "create 0mq context failed";
    zmq_ctx_set(context_, ZMQ_MAX_SOCKETS, 65536);
  }
  mu_.unlock();

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

  // bind
  Bind(my_node_, 40);
  VLOG(1) << "Bind to " << my_node_.DebugString();

  // connect to scheduler
  Connect(scheduler_node_);

  // start receiving
  receiver_thread_ = std::thread(&WorkerMailbox::Receiving, this);

  // let the scheduler know myself
  Message msg;
  msg.meta.flag = Flag::kMailboxControl;
  msg.meta.sender = my_node_.id;
  msg.meta.recver = scheduler_node_.id;

  Control ctrl;
  ctrl.flag = MailboxFlag::kRegister;
  ctrl.node = my_node_;

  SArrayBinStream bin;
  bin << ctrl;
  msg.AddData(bin.ToSArray());
  // VLOG(1) << "Worker: my_node_.id = " << std::to_string(my_node_.id);
  Send(msg);

  // wait until ready
  while (!ready_.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  heartbeat_thread_ = std::thread(&WorkerMailbox::Heartbeat, this);
  start_time_ = time(NULL);

  VLOG(1) << my_node_.DebugString() << " started";
}

void WorkerMailbox::Stop() {
  BasicMailbox::Stop();
  heartbeat_thread_.join();
}

// heartbeat
std::thread heartbeat_thread_;

void WorkerMailbox::Heartbeat() {
  // heartbeat interval, make it self-defined in the future
  const int interval = 0;
  while (interval > 0 && ready_.load()) {
    std::this_thread::sleep_for(std::chrono::seconds(interval));
    if (!ready_.load())
      break;
    Message msg;
    msg.meta.sender = my_node_.id;
    msg.meta.recver = scheduler_node_.id;
    msg.meta.flag = Flag::kMailboxControl;
    Control ctrl;
    ctrl.flag = MailboxFlag::kHeartbeat;
    SArrayBinStream bin;
    bin << ctrl;
    msg.AddData(bin.ToSArray());
    Send(msg);
  }
}

void WorkerMailbox::HandleBarrierMsg() {
  VLOG(2) << "Barrier at worker";
  barrier_finish_ = true;
}

void WorkerMailbox::HandleRegisterMsg(Message *msg, Node &recovery_node) {
  // reference:
  auto dead_nodes = GetDeadNodes(heartbeat_timeout_);
  std::unordered_set<int> dead_set(dead_nodes.begin(), dead_nodes.end());
  UpdateID(msg, &dead_set, recovery_node);

  // worker connected to all other workers (get the info from scheduler)
  VLOG(1) << "[Worker]In HandleRegisterMsg: nodes.size() = "
          << std::to_string(nodes_.size());
  for (const auto &node : nodes_) {
    std::string addr_str = node.hostname + ":" + std::to_string(node.port);
    if (connected_nodes_.find(addr_str) == connected_nodes_.end()) {
      Connect(node);
      connected_nodes_[addr_str] = node.id;
    }
  }
  LOG(INFO) << my_node_.DebugString() << " is connected to others";
  ready_ = true;
}

void WorkerMailbox::UpdateID(Message *msg,
                             std::unordered_set<int> *deadnodes_set,
                             Node &recovery_node) {
  // update my id
  SArrayBinStream bin;
  bin.FromSArray(msg->data[1]);
  bin >> nodes_;
  VLOG(1) << "[Worker]In UpdateID(): nodes.size() = "
          << std::to_string(nodes_.size());
  for (size_t i = 0; i < nodes_.size(); ++i) {
    const auto &node = nodes_[i];
    if (my_node_.hostname == node.hostname && my_node_.port == node.port) {
      my_node_ = node;
    }
  }
}

void WorkerMailbox::Receiving() {
  Node recovery_node; // store recovery nodes

  while (true) {
    Message msg;
    int recv_bytes = Recv(&msg);
    CHECK_NE(recv_bytes, -1);
    // duplicated message, TODO
    VLOG(1) << "Received msg: " << msg.DebugString();
    if (msg.meta.flag == Flag::kMailboxControl) {
      Control ctrl;
      SArrayBinStream bin;
      bin.FromMsg(msg);
      bin >> ctrl;
      if (ctrl.flag == MailboxFlag::kExit) {
        ready_ = false;
        VLOG(1) << my_node_.DebugString() << " is stopped";
        break;
      } else if (ctrl.flag == MailboxFlag::kBarrier) {
        HandleBarrierMsg();
      } else if (ctrl.flag == MailboxFlag::kRegister) {
        HandleRegisterMsg(&msg, recovery_node);
      }
    } else {
      CHECK(queue_map_.find(msg.meta.recver) != queue_map_.end());
      queue_map_[msg.meta.recver]->Push(std::move(msg));
    }
  }
}
} // namespace xyz