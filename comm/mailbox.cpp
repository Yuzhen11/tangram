#include "comm/mailbox.hpp"

namespace xyz {

inline void FreeData(void* data, void* hint) {
  if (hint == NULL) {
    delete[] static_cast<char*>(data);
  } else {
    delete static_cast<third_party::SArray<char>*>(hint);
  }
}

Mailbox::Mailbox(bool is_scheduler, Node scheduler_node, int num_workers)
    : is_scheduler_(is_scheduler), scheduler_node_(scheduler_node), num_workers_(num_workers){
      if (is_scheduler_) {
        my_node_ = scheduler_node_;
      }
}

Mailbox::~Mailbox() {
}

void Mailbox::Bind(const Node& node, int max_retry) {
  receiver_ = zmq_socket(context_, ZMQ_ROUTER);
  CHECK(receiver_ != nullptr) << "create receiver socket failed: " << zmq_strerror(errno);
  std::string address = "tcp://*:" + std::to_string(node.port);
  for (int i = 0; i < max_retry; i++) {
    if (zmq_bind(receiver_, address.c_str()) == 0)
      break;
    if (i == max_retry - 1)
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
  std::string my_id = "ps" + std::to_string(my_node_.id);
  zmq_setsockopt(sender, ZMQ_IDENTITY, my_id.data(), my_id.size());
  std::string addr = "tcp://" + node.hostname + ":" + std::to_string(node.port);
  if (zmq_connect(sender, addr.c_str()) != 0) {
    LOG(FATAL) << "connect to " + addr + " failed: " << zmq_strerror(errno);
  }
  senders_[node.id] = sender;
}

void Mailbox::BindAndConnect() {
  context_ = zmq_ctx_new();
  CHECK(context_ != nullptr) << "create zmq context failed";
  zmq_ctx_set(context_, ZMQ_MAX_SOCKETS, 65536);

  Bind(scheduler_node_, 1);
  VLOG(1) << "Finished binding";
  Connect(scheduler_node_);
  VLOG(1) << "Finished connecting";
}

void Mailbox::RegisterQueue(uint32_t queue_id, ThreadsafeQueue<Message>* const queue) {
  std::lock_guard<std::mutex> lk(mu_);
  CHECK(queue_map_.find(queue_id) == queue_map_.end());
  queue_map_.insert({queue_id, queue});
}

void Mailbox::DeregisterQueue(uint32_t queue_id) {
  std::lock_guard<std::mutex> lk(mu_);
  CHECK(queue_map_.find(queue_id) != queue_map_.end());
  queue_map_.erase(queue_id);
}

int Mailbox::Send(const Message& msg) {
  std::lock_guard<std::mutex> lk(mu_);
  // find the socket
  int recver_id = msg.meta.recver;
  auto it = senders_.find(recver_id);
  if (it == senders_.end()) {
    LOG(WARNING) << "there is no socket to node " << recver_id;
    return -1;
  }
  void* socket = it->second;

  // send meta
  int meta_size = sizeof(Meta);
  int tag = ZMQ_SNDMORE;
  int num_data = msg.data.size();
  if (num_data == 0)
    tag = 0;
  char* meta_buf = new char[meta_size];
  memcpy(meta_buf, &msg.meta, meta_size);
  zmq_msg_t meta_msg;
  zmq_msg_init_data(&meta_msg, meta_buf, meta_size, FreeData, NULL);
  while (true) {
    if (zmq_msg_send(&meta_msg, socket, tag) == meta_size)
      break;
    if (errno == EINTR)
      continue;
    LOG(WARNING) << "failed to send message to node [" << recver_id << "] errno: " << errno << " " << zmq_strerror(errno);
    return -1;
  }
  zmq_msg_close(&meta_msg);
  int send_bytes = meta_size;

  // send data
  VLOG(1) << "Node " << my_node_.id << " starts sending data: " << msg.DebugString();
  for (int i = 0; i < num_data; ++i) {
    zmq_msg_t data_msg;
    third_party::SArray<char>* data = new third_party::SArray<char>(msg.data[i]);
    int data_size = data->size();
    zmq_msg_init_data(&data_msg, data->data(), data->size(), FreeData, data);
    if (i == num_data - 1)
      tag = 0;
    while (true) {
      if (zmq_msg_send(&data_msg, socket, tag) == data_size)
        break;
      if (errno == EINTR)
        continue;
      LOG(WARNING) << "failed to send message to node [" << recver_id << "] errno: " << errno << " " << zmq_strerror(errno)
                   << ". " << i << "/" << num_data;
      return -1;
    }
    zmq_msg_close(&data_msg);
    send_bytes += data_size;
  }
  return send_bytes;
}

int Mailbox::Recv(Message* msg) {
  msg->data.clear();
  size_t recv_bytes = 0;
  for (int i = 0; ; ++i) {
    zmq_msg_t* zmsg = new zmq_msg_t;
    CHECK(zmq_msg_init(zmsg) == 0) << zmq_strerror(errno);
    while (true) {
      if (zmq_msg_recv(zmsg, receiver_, 0) != -1)
        break;
      if (errno == EINTR)
        continue;
      LOG(WARNING) << "failed to receive message. errno: " << errno << " " << zmq_strerror(errno);
      return -1;
    }

    size_t size = zmq_msg_size(zmsg);
    recv_bytes += size;

    if (i == 0) {
      // identify, don't care
      CHECK(zmq_msg_more(zmsg));
      zmq_msg_close(zmsg);
      delete zmsg;
    } else if (i == 1) {
      // Unpack the meta
      Meta* meta = CHECK_NOTNULL((Meta*) zmq_msg_data(zmsg));
      msg->meta = *meta;
      zmq_msg_close(zmsg);
      bool more = zmq_msg_more(zmsg);
      delete zmsg;
      if (!more)
        break;
    } else {
      // data, zero-copy
      char* buf = CHECK_NOTNULL((char*) zmq_msg_data(zmsg));
      third_party::SArray<char> data;
      data.reset(buf, size, [zmsg, size](char* buf) {
        zmq_msg_close(zmsg);
        delete zmsg;
      });
      msg->data.push_back(data);
      if (!zmq_msg_more(zmsg)) {
        break;
      }
    }
  }
  return recv_bytes;
}

void Mailbox::Barrier() {
  Message barrier_msg;
  barrier_msg.meta.sender = my_node_.id;
  barrier_msg.meta.recver = scheduler_node_.id;
  barrier_msg.meta.flag = Flag::kBarrier;
  barrier_finish_ = false;
  Send(barrier_msg);

  while (!barrier_finish_) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

void Mailbox::Start() {
  // start zmq
  mu_.lock();
  if (context_ == nullptr) {
    context_ = zmq_ctx_new();
    CHECK(context_ != NULL) << "create 0mq context failed";
    zmq_ctx_set(context_, ZMQ_MAX_SOCKETS, 65536);
  }
  mu_.unlock();

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
  Bind(my_node_, is_scheduler_ ? 1 : 40);
  VLOG(1) << "Bind to " << my_node_.DebugString();

  // connect to scheduler
  Connect(scheduler_node_);

  // start receiving
  receiver_thread_ = std::thread(&Mailbox::Receiving, this);

  if (!is_scheduler_) {
    // let the scheduler know myself
    Message msg;
    msg.meta.sender = my_node_.id;
    msg.meta.recver = scheduler_node_.id;
    msg.meta.flag = Flag::kRegister;
    msg.meta.node = my_node_;
    VLOG(1) << "Worker: my_node_.id = " << std::to_string(my_node_.id);
    Send(msg);
  }

  // wait until ready
  while (!ready_.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  if (!is_scheduler_) {
    heartbeat_thread_ = std::thread(&Mailbox::Heartbeat, this);
  }
  
  start_time_ = time(NULL);

  VLOG(1) << my_node_.DebugString() << " started";
}

void Mailbox::CloseSockets() {
  // Kill all the registered threads
  Message exit_msg;
  exit_msg.meta.recver = my_node_.id;
  exit_msg.meta.flag = Flag::kExit;
  for (auto& queue : queue_map_) {
    queue.second->Push(exit_msg);
  }
  // close sockets
  int linger = 0;  // infinite linger period. Wait for all pending messages to be sent.
  int rc = zmq_setsockopt(receiver_, ZMQ_LINGER, &linger, sizeof(linger));
  CHECK(rc == 0 || errno == ETERM);
  CHECK_EQ(zmq_close(receiver_), 0);
  for (auto& it : senders_) {
    int rc = zmq_setsockopt(it.second, ZMQ_LINGER, &linger, sizeof(linger));
    CHECK(rc == 0 || errno == ETERM);
    CHECK_EQ(zmq_close(it.second), 0);
  }
  zmq_ctx_destroy(context_);
}

void Mailbox::Stop() {
  VLOG(1) << my_node_.DebugString() << " is stopping";
  //Barrier();
  // stop threads
  Message exit;
  exit.meta.flag = Flag::kExit;
  exit.meta.recver = my_node_.id;
  int ret = Send(exit);
  CHECK_NE(ret, -1);
  receiver_thread_.join();
  if (!is_scheduler_) {
  	heartbeat_thread_.join();
  }
  // if (resender_) delete resender_;
  // close sockets
  CloseSockets();
}

void Mailbox::HandleBarrierMsg(Message* msg) {
  if (is_scheduler_) {
    barrier_count_++;
    if (barrier_count_ == nodes_.size()) {
      // notify all nodes that barrier finished
      for (auto& node : nodes_) {
        Message barrier_msg;
        barrier_msg.meta.sender = my_node_.id;
        barrier_msg.meta.recver = node.id;
        barrier_msg.meta.flag = Flag::kBarrier;
        Send(barrier_msg);
      }
    }
  }
  // worker side, barrier finished
  else {
    barrier_finish_ = true;
  }
}

void Mailbox::HandleRegisterMsg(Message* msg, std::vector<Node>& nodes, Node& recovery_node) {
  // reference:
  auto dead_nodes = GetDeadNodes(heartbeat_timeout_);
  std::unordered_set<int> dead_set(dead_nodes.begin(), dead_nodes.end());
  UpdateID(msg, &dead_set, nodes, recovery_node);

  if (is_scheduler_) {
    HandleRegisterMsgAtScheduler(msg, nodes, recovery_node);
  } else {
    // worker connected to all other workers (get the info from scheduler)
    VLOG(1) << "[Worker]In HandleRegisterMsg: nodes.size() = " << std::to_string(nodes.size());
    for (const auto& node : nodes) {
      std::string addr_str = node.hostname + ":" + std::to_string(node.port);
      if (connected_nodes_.find(addr_str) == connected_nodes_.end()) {
        Connect(node);
        connected_nodes_[addr_str] = node.id;
      }
    }
    VLOG(1) << my_node_.DebugString() << " is connected to others";
    ready_ = true;
  }
}

void Mailbox::UpdateID(Message* msg, std::unordered_set<int>* deadnodes_set, std::vector<Node>& nodes, Node& recovery_node) {
  // assign an id
  if (msg->meta.sender == Node::kEmpty) {
    CHECK(is_scheduler_);
    if (nodes.size() < num_workers_) {
      nodes.push_back(msg->meta.node);
    } else {
      // some node dies and restarts
      CHECK(ready_.load());
      for (size_t i = 0; i < nodes.size() - 1; ++i) {
        const auto& node = nodes[i];
        if (deadnodes_set->find(node.id) != deadnodes_set->end()) {
          auto& temp_node = msg->meta.node;
          // assign previous node id
          temp_node.id = node.id;
          temp_node.is_recovery = true;
          VLOG(1) << "replace dead node " << node.DebugString()
                     << " by node " << temp_node.DebugString();
          nodes[i] = temp_node;
          recovery_node = temp_node;
          break;
        }
      }
    }
  }
  // update my id
  else if (!is_scheduler_) {
    SArrayBinStream bin;
    bin.FromMsg(*msg);
    bin >> nodes;
    VLOG(1) << "[Worker]In UpdateID(): nodes.size() = " << std::to_string(nodes.size());
    for (size_t i = 0; i < nodes.size(); ++i) {
      const auto& node = nodes[i];
      if (my_node_.hostname == node.hostname && my_node_.port == node.port) {
        my_node_ = node;
      }
    }
  }
}

void Mailbox::HandleRegisterMsgAtScheduler(Message* msg, std::vector<Node>& nodes, Node& recovery_node) {
  time_t t = time(NULL);
  VLOG(1) << "HandleRegisterMsgAtScheduler";
  if (nodes.size() == num_workers_) {
    VLOG(1) << "nodes.size() == num_workers_";
    // assign node id (dummy ranking, id from 1 to num_workers_)
    int id = 0;
    for (auto& node : nodes) {
      id++;
      std::string node_host_ip = node.hostname + ":" + std::to_string(node.port);
      if (connected_nodes_.find(node_host_ip) == connected_nodes_.end()) {
        CHECK_EQ(node.id, Node::kEmpty);
        VLOG(1) << "assign id=" << id << " to node " << node.DebugString();
        node.id = id;
        Connect(node);
        UpdateHeartbeat(node.id, t);
        connected_nodes_[node_host_ip] = id;
      } else {
        shared_node_mapping_[id] = connected_nodes_[node_host_ip];
        node.id = connected_nodes_[node_host_ip];
      }
    }
    // put nodes into msg
    SArrayBinStream bin;
    bin << nodes;
    Message back = bin.ToMsg();
    VLOG(1) << "back.data.size() = " << std::to_string(back.data.size());
    VLOG(1) << "back.data[0].size() = " << std::to_string(back.data[0].size());
    back.meta.flag = Flag::kRegister;
    for (int r : GetNodeIDs()) {
      if (shared_node_mapping_.find(r) == shared_node_mapping_.end()) {
        back.meta.recver = r;
        Send(back);
      }
    }
    VLOG(1) << "the scheduler is connected to " << num_workers_ << " workers";
    ready_ = true;
  } 
  else if (recovery_node.is_recovery) {
    VLOG(1) << "recovery_node.is_recovery == true";
    auto dead_nodes = GetDeadNodes(heartbeat_timeout_);
    std::unordered_set<int> dead_set(dead_nodes.begin(), dead_nodes.end());
    // send back the recovery node
    Connect(recovery_node);
    UpdateHeartbeat(recovery_node.id, t);
    for (int r : GetNodeIDs()) {
      if (r != recovery_node.id
          && dead_set.find(r) != dead_set.end()) {
        // do not send anything to dead node
        continue;
      }
      // only send recovery_node to nodes already exist
      // but send all nodes to the recovery_node
      SArrayBinStream bin;
      if (r == recovery_node.id) {
        bin << nodes; 
      }
      else {
        std::vector<Node> temp = {recovery_node};
        bin << temp;
      }
      Message back = bin.ToMsg();
      back.meta.flag = Flag::kRegister;
      back.meta.recver = r;
      Send(back);
    }
  }
  VLOG(1) << "Finished HandleRegisterMsgAtScheduler";
}

void Mailbox::HandleHeartbeat(Message* msg) {
  if (is_scheduler_) {
    time_t t = time(NULL);
    std::lock_guard<std::mutex> lk(heartbeat_mu_);
    heartbeats_[msg->meta.node.id] = t;
  }
}

void Mailbox::UpdateHeartbeat(int node_id, time_t t) {
  std::lock_guard<std::mutex> lk(heartbeat_mu_);
  heartbeats_[node_id] = t;
}

void Mailbox::Receiving() {
  std::vector<Node> nodes; // store worker nodes
  Node recovery_node;  // store recovery nodes

  while (true) {
    Message msg;
    int recv_bytes = Recv(&msg);
    CHECK_NE(recv_bytes, -1);
    // duplicated message, TODO
    // if (resender_ && resender_->AddIncomming(msg)) continue;

    if (msg.meta.flag == Flag::kExit) {
      ready_ = false;
      VLOG(1) << my_node_.DebugString() << " is stopped";
      break;
    } 
    else if (msg.meta.flag == Flag::kBarrier) {
      HandleBarrierMsg(&msg);
    }
    else if (msg.meta.flag == Flag::kRegister) {
      VLOG(1) << "In Receiving(), msg.meta.node.id = " << std::to_string(msg.meta.node.id);
      HandleRegisterMsg(&msg, nodes, recovery_node);
    }
    else if (msg.meta.flag == Flag::kHeartbeat) {
      HandleHeartbeat(&msg);
    }
    else {
      CHECK(queue_map_.find(msg.meta.recver) != queue_map_.end());
      queue_map_[msg.meta.recver]->Push(std::move(msg));
    }
  }
}

void Mailbox::Heartbeat() {
  // heartbeat interval, make it self-defined in the future
  const int interval = 1;
  while (interval > 0 && ready_.load()) {
    std::this_thread::sleep_for(std::chrono::seconds(interval));
    Message msg;
    msg.meta.recver = scheduler_node_.id;
    msg.meta.flag = Flag::kHeartbeat;
    msg.meta.node = my_node_;
    Send(msg);
  }
}

const std::vector<int> Mailbox::GetNodeIDs() const {
  std::vector<int> temp;
  for (auto it : connected_nodes_) {
    temp.push_back(it.second);
  }
  return temp;
}

std::vector<int> Mailbox::GetDeadNodes(int timeout) {
  std::vector<int> dead_nodes;
  if (!ready_ || timeout == 0) return dead_nodes;

  time_t curr_time = time(NULL);
  const auto nodes = GetNodeIDs();
  {
    std::lock_guard<std::mutex> lk(heartbeat_mu_);
    for (int r : nodes) {
      auto it = heartbeats_.find(r);
      if ((it == heartbeats_.end() || it->second + timeout < curr_time)
            && start_time_ + timeout < curr_time) {
        dead_nodes.push_back(r);
      }
    }
  }
  return dead_nodes;
}

}  // namespace xyz
