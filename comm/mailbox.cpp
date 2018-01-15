#include "comm/mailbox.hpp"

#include "base/third_party/network_utils.h"

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
  Bind(my_node_, is_scheduler_ ? 0 : 40);
  VLOG(1) << "Bind to " << my_node_.DebugString();

  // connect to scheduler
  Connect(scheduler_node_);

  // start receiving
  receiver_thread_ = std::thread(&Mailbox::Receiving, this);

  if (!is_scheduler_) {
    // let the scheduler know myself
    Message msg;
    msg.meta.recver = scheduler_node_.id;
    msg.meta.flag = Flag::kRegister;
    msg.meta.node = my_node_;
    Send(msg);
  }

  // wait until ready
  while (!ready_.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  if (!is_scheduler_) {
    heartbeat_thread_ = std::thread(&Mailbox::Heartbeat, this);
  }
}

void Mailbox::Stop() {
  VLOG(1) << my_node_.DebugString() << " is stopping";
  // stop threads
  Message exit;
  exit.meta.flag = Flag::kExit;
  exit.meta.recver = my_node_.id;
  int ret = Send(exit);
  CHECK_NE(ret, -1);
  receiver_thread_.join();
  if (!is_scheduler_) heartbeat_thread_.join();
  // if (resender_) delete resender_;

  // close sockets
  int linger = 0;
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

void Mailbox::HandleRegisterMsg(Message* msg) {
  if (is_scheduler_) {
    auto& node = msg->meta.node;
    // Can this actually change the worker node's id???
    node.id = nodes_.size();
    Connect(node);
    nodes_.push_back(node);
    // update heartbeat of this node
    std::lock_guard<std::mutex> lk(heartbeat_mu_);
    heartbeats_[node.id] = time(NULL);

    if (nodes_.size() == num_workers_) {
      // notify all the nodes of their id
      for (auto& node : nodes_) {
        Message msg;
        msg.meta.sender = my_node_.id;
        msg.meta.recver = node.id;
        msg.meta.flag = Flag::kRegister;
        // TODO: embed all nodes' info in the msg and send to each worker
        Send(msg);
      }
    }
  }
  // worker
  else {
    ready_ = true;
    // TODO: store other worker nodes' info
  }
}

void Mailbox::HandleHeartbeat(Message* msg) {
  if (is_scheduler_) {
    time_t t = time(NULL);
    std::lock_guard<std::mutex> lk(heartbeat_mu_);
    heartbeats_[msg->meta.node.id] = t;
  }
}

void Mailbox::Receiving() {
  while (true) {
    Message msg;
    int recv_bytes = Recv(&msg);
    CHECK_NE(recv_bytes, -1);
    // duplicated message, TODO
    // if (resender_ && resender_->AddIncomming(msg)) continue;

    if (msg.meta.flag == Flag::kExit) {
      VLOG(1) << my_node_.DebugString() << " is stopped";
      ready_ = false;
      break;
    } 
    else if (msg.meta.flag == Flag::kBarrier) {
      HandleBarrierMsg(&msg);
    }
    else if (msg.meta.flag == Flag::kRegister) {
      HandleRegisterMsg(&msg);
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
    msg.meta.timestamp = timestamp_++;
    Send(msg);
  }
}
}  // namespace xyz
