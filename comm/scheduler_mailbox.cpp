#include "comm/scheduler_mailbox.hpp"

namespace xyz {

SchedulerMailbox::SchedulerMailbox(Node scheduler_node, int num_workers)
    : BasicMailbox(scheduler_node), num_workers_(num_workers) {}

SchedulerMailbox::~SchedulerMailbox() {}

void SchedulerMailbox::Start() {
  // start zmq
  mu_.lock();
  if (context_ == nullptr) {
    context_ = zmq_ctx_new();
    CHECK(context_ != NULL) << "create 0mq context failed";
    zmq_ctx_set(context_, ZMQ_MAX_SOCKETS, 65536);
  }
  mu_.unlock();

  my_node_ = scheduler_node_;

  // bind
  Bind(my_node_, 1);
  VLOG(2) << "Bind to " << my_node_.DebugString();

  // connect to scheduler
  Connect(scheduler_node_);

  // start receiving
  receiver_thread_ = std::thread(&SchedulerMailbox::Receiving, this);

  // wait until ready
  while (!ready_.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  // Check the heartbeats of workers and find out dead nodes
  heartbeat_thread_ =
      std::thread(&SchedulerMailbox::CheckHeartbeat, this, kHeartbeatTimeout);

  start_time_ = time(NULL);
  VLOG(2) << my_node_.DebugString() << " started";
}

void SchedulerMailbox::HandleBarrierMsg() {
  barrier_count_++;
  VLOG(2) << "Barrier at scheduler, count: " << barrier_count_;
  if (barrier_count_ == nodes_.size()) {
    LOG(INFO) << "Collect all barrier message at scheduler";
    barrier_count_ = 0;
    // notify all nodes that barrier finished
    for (auto &node : nodes_) {
      Message barrier_msg;
      barrier_msg.meta.sender = my_node_.id;
      barrier_msg.meta.recver = node.id;
      barrier_msg.meta.flag = Flag::kMailboxControl;
      Control ctrl;
      ctrl.flag = MailboxFlag::kBarrier;
      SArrayBinStream bin;
      bin << ctrl;
      barrier_msg.AddData(bin.ToSArray());
      Send(barrier_msg);
    }
  }
}

void SchedulerMailbox::HandleRegisterMsg(Message *msg, Node &recovery_node) {
  // reference:
  auto dead_nodes = GetDeadNodes(kHeartbeatTimeout);
  std::unordered_set<int> dead_set(dead_nodes.begin(), dead_nodes.end());
  UpdateID(msg, &dead_set, recovery_node);

  if (nodes_.size() == num_workers_) {
    LOG(INFO) << num_workers_ << " nodes registered at scheduler.";
    // assign node id (dummy ranking, id from 1 to num_workers_)
    int id = 0;
    for (auto &node : nodes_) {
      id++;
      std::string node_host_ip =
          node.hostname + ":" + std::to_string(node.port);
      if (connected_nodes_.find(node_host_ip) == connected_nodes_.end()) {
        CHECK_EQ(node.id, Node::kEmpty);
        node.id = id;
        Connect(node);
        UpdateHeartbeat(node.id);
        connected_nodes_[node_host_ip] = id;
      } else {
        shared_node_mapping_[id] = connected_nodes_[node_host_ip];
        node.id = connected_nodes_[node_host_ip];
      }
    }
    // put nodes into msg
    SArrayBinStream ctrl_bin;
    Control ctrl;
    ctrl.flag = MailboxFlag::kRegister;
    ctrl_bin << ctrl;
    SArrayBinStream nodes_bin;
    nodes_bin << nodes_;
    Message back_msg;
    back_msg.AddData(ctrl_bin.ToSArray());
    back_msg.AddData(nodes_bin.ToSArray());
    for (int r : GetNodeIDs()) {
      if (shared_node_mapping_.find(r) == shared_node_mapping_.end()) {
        back_msg.meta.recver = r;
        back_msg.meta.flag = Flag::kMailboxControl;
        Send(back_msg);
      }
    }
    VLOG(1) << "the scheduler is connected to " << num_workers_ << " workers";
    ready_ = true;
  }

  else if (recovery_node.is_recovery) {
    VLOG(1) << "recovery_node.is_recovery == true";
    auto dead_nodes = GetDeadNodes(kHeartbeatTimeout);
    std::unordered_set<int> dead_set(dead_nodes.begin(), dead_nodes.end());
    // send back the recovery node
    Connect(recovery_node);
    UpdateHeartbeat(recovery_node.id);
    for (int r : GetNodeIDs()) {
      if (r != recovery_node.id && dead_set.find(r) != dead_set.end()) {
        // do not send anything to dead node
        continue;
      }
      // only send recovery_node to nodes already exist
      // but send all nodes to the recovery_node
      SArrayBinStream nodes_bin;
      if (r == recovery_node.id) {
        nodes_bin << nodes_;
      } else {
        std::vector<Node> temp = {recovery_node};
        nodes_bin << temp;
      }
      SArrayBinStream ctrl_bin;
      Control ctrl;
      ctrl.flag = MailboxFlag::kRegister;
      ctrl_bin << ctrl;
      Message back_msg;
      back_msg.meta.recver = r;
      back_msg.meta.flag = Flag::kMailboxControl;
      back_msg.AddData(ctrl_bin.ToSArray());
      back_msg.AddData(nodes_bin.ToSArray());
      Send(back_msg);
    }
  }
}

void SchedulerMailbox::UpdateID(Message *msg,
                                std::unordered_set<int> *deadnodes_set,
                                Node &recovery_node) {
  // assign an id
  CHECK_EQ(msg->meta.sender, Node::kEmpty);
  SArrayBinStream bin;
  bin.FromMsg(*msg);
  Control ctrl;
  bin >> ctrl;

  if (nodes_.size() < num_workers_) {
    nodes_.push_back(ctrl.node);
  } else {
    // some node dies and restarts
    CHECK(ready_.load());
    for (size_t i = 0; i < nodes_.size() - 1; ++i) {
      const auto &node = nodes_[i];
      if (deadnodes_set->find(node.id) != deadnodes_set->end()) {
        auto &temp_node = ctrl.node;
        // assign previous node id
        temp_node.id = node.id;
        temp_node.is_recovery = true;
        VLOG(1) << "replace dead node " << node.DebugString() << " by node "
                << temp_node.DebugString();
        nodes_[i] = temp_node;
        recovery_node = temp_node;
        break;
      }
    }
  }
}

std::vector<int> SchedulerMailbox::GetDeadNodes(int timeout) {
  std::vector<int> dead_nodes;
  if (!ready_ || timeout == 0)
    return dead_nodes;

  time_t curr_time = time(NULL);
  const auto nodes = GetNodeIDs();
  {
    std::lock_guard<std::mutex> lk(heartbeat_mu_);
    for (int r : nodes) {
      auto it = heartbeats_.find(r);
      if ((it == heartbeats_.end() || it->second + timeout < curr_time) &&
          start_time_ + timeout < curr_time) {
        dead_nodes.push_back(r);
      }
    }
  }
  return dead_nodes;
}

void SchedulerMailbox::CheckHeartbeat(int time_out) {
  while (ready_.load() && kHeartbeatCheckInterval > 0) {
    std::this_thread::sleep_for(std::chrono::seconds(kHeartbeatCheckInterval));
    if (!ready_.load())
      break;

    std::vector<int> deadnodes = GetDeadNodes(time_out);
    if (!deadnodes.empty()) {
      // TODO: start a new worker node
      VLOG(1) << "Detected " << std::to_string(deadnodes.size()) << " deadnode";
      Message msg;
      msg.meta.recver = 0;
      msg.meta.flag = Flag::kOthers;
      SArrayBinStream ctrl_bin, bin;
      ctrl_bin << ScheduleFlag::kRecovery;
      bin << deadnodes;
      msg.AddData(ctrl_bin.ToSArray());
      msg.AddData(bin.ToSArray());
      Send(msg);
    }
  }
}

void SchedulerMailbox::UpdateHeartbeat(int node_id) {
  time_t t = time(NULL);
  std::lock_guard<std::mutex> lk(heartbeat_mu_);
  heartbeats_[node_id] = t;
  VLOG(1) << "Heartbeat from node_id: " << std::to_string(node_id)
          << " time: " << std::to_string(t);
}

const std::vector<int> SchedulerMailbox::GetNodeIDs() {
  std::vector<int> temp;
  for (auto it : connected_nodes_) {
    temp.push_back(it.second);
  }
  return temp;
}

void SchedulerMailbox::Receiving() {
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
      } else if (ctrl.flag == MailboxFlag::kHeartbeat) {
        UpdateHeartbeat(msg.meta.sender);
      }
    } else {
      CHECK(queue_map_.find(msg.meta.recver) != queue_map_.end())
          << msg.meta.recver;
      queue_map_[msg.meta.recver]->Push(std::move(msg));
    }
  }
}
} // namespace xyz
