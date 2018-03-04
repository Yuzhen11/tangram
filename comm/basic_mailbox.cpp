#include "comm/basic_mailbox.hpp"
#include "core/queue_node_map.hpp"

namespace xyz {

SArrayBinStream &operator<<(xyz::SArrayBinStream &stream, const Control &ctrl) {
  stream << static_cast<char>(ctrl.flag) << ctrl.node << ctrl.is_recovery;
  return stream;
}

SArrayBinStream &operator>>(xyz::SArrayBinStream &stream, Control &ctrl) {
  char ch;
  stream >> ch;
  ctrl.flag = static_cast<MailboxFlag>(ch);
  stream >> ctrl.node >> ctrl.is_recovery;
  return stream;
}

BasicMailbox::BasicMailbox(Node scheduler_node)
    : scheduler_node_(scheduler_node) {}

BasicMailbox::~BasicMailbox() {}

void BasicMailbox::RegisterQueue(uint32_t queue_id,
                                 ThreadsafeQueue<Message> *const queue) {
  std::lock_guard<std::mutex> lk(mu_);
  CHECK(queue_map_.find(queue_id) == queue_map_.end());
  queue_map_.insert({queue_id, queue});
}

void BasicMailbox::DeregisterQueue(uint32_t queue_id) {
  std::lock_guard<std::mutex> lk(mu_);
  CHECK(queue_map_.find(queue_id) != queue_map_.end());
  queue_map_.erase(queue_id);
}

void BasicMailbox::Stop() {
  VLOG(2) << my_node_.DebugString() << " is stopping";
  // Barrier();

  // stop threads
  Message exit;
  exit.meta.flag = Flag::kMailboxControl;
  exit.meta.recver = my_node_.id;
  Control ctrl;
  ctrl.flag = MailboxFlag::kExit;
  SArrayBinStream bin;
  bin << ctrl;
  exit.AddData(bin.ToSArray());

  int ret = Send(exit);
  CHECK_NE(ret, -1);
  receiver_thread_.join();
  // close sockets
  CloseSockets();
  LOG(INFO) << my_node_.DebugString() << " is stopped";

  heartbeat_thread_.join();
}

// return # of bytes sended
int BasicMailbox::Send(const Message &msg) {
  std::lock_guard<std::mutex> lk(mu_);
  // find the socket
  int recver_id = msg.meta.flag == Flag::kOthers ? GetNodeId(msg.meta.recver)
                                                 : msg.meta.recver; // TODO
  auto it = senders_.find(recver_id);
  if (it == senders_.end()) {
    LOG(WARNING) << "there is no socket to node " << recver_id;
    LOG(INFO) << msg.DebugString();
    return -1;
  }
  void *socket = it->second;

  // send meta
  int meta_size = sizeof(Meta);
  int tag = ZMQ_SNDMORE;
  int num_data = msg.data.size();
  if (num_data == 0)
    tag = 0;
  char *meta_buf = new char[meta_size];
  memcpy(meta_buf, &msg.meta, meta_size);
  zmq_msg_t meta_msg;
  zmq_msg_init_data(&meta_msg, meta_buf, meta_size, FreeData, NULL);
  while (true) {
    if (zmq_msg_send(&meta_msg, socket, tag) == meta_size)
      break;
    if (errno == EINTR)
      continue;
    LOG(WARNING) << "failed to send message to node [" << recver_id
                 << "] errno: " << errno << " " << zmq_strerror(errno);
    return -1;
  }
  zmq_msg_close(&meta_msg);
  int send_bytes = meta_size;

  // send data
  for (int i = 0; i < num_data; ++i) {
    zmq_msg_t data_msg;
    third_party::SArray<char> *data =
        new third_party::SArray<char>(msg.data[i]);
    int data_size = data->size();
    zmq_msg_init_data(&data_msg, data->data(), data->size(), FreeData, data);
    if (i == num_data - 1)
      tag = 0;
    while (true) {
      if (zmq_msg_send(&data_msg, socket, tag) == data_size)
        break;
      if (errno == EINTR)
        continue;
      LOG(WARNING) << "failed to send message to node [" << recver_id
                   << "] errno: " << errno << " " << zmq_strerror(errno) << ". "
                   << i << "/" << num_data;
      return -1;
    }
    zmq_msg_close(&data_msg);
    send_bytes += data_size;
  }
  return send_bytes;
}

int BasicMailbox::Recv(Message *msg) {
  msg->data.clear();
  size_t recv_bytes = 0;
  for (int i = 0;; ++i) {
    zmq_msg_t *zmsg = new zmq_msg_t;
    CHECK(zmq_msg_init(zmsg) == 0) << zmq_strerror(errno);
    while (true) {
      if (zmq_msg_recv(zmsg, receiver_, 0) != -1)
        break;
      if (errno == EINTR)
        continue;
      LOG(WARNING) << "failed to receive message. errno: " << errno << " "
                   << zmq_strerror(errno);
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
      Meta *meta = CHECK_NOTNULL((Meta *)zmq_msg_data(zmsg));
      msg->meta = *meta;
      zmq_msg_close(zmsg);
      bool more = zmq_msg_more(zmsg);
      delete zmsg;
      if (!more)
        break;
    } else {
      // data, zero-copy
      char *buf = CHECK_NOTNULL((char *)zmq_msg_data(zmsg));
      third_party::SArray<char> data;
      data.reset(buf, size, [zmsg, size](char *buf) {
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

void BasicMailbox::Barrier() {
  Message barrier_msg;
  barrier_msg.meta.flag = Flag::kMailboxControl;
  barrier_msg.meta.sender = my_node_.id;
  barrier_msg.meta.recver = scheduler_node_.id;

  Control ctrl;
  ctrl.flag = MailboxFlag::kBarrier;
  SArrayBinStream bin;
  bin << ctrl;
  barrier_msg.AddData(bin.ToSArray());
  barrier_finish_ = false;
  Send(barrier_msg);

  while (!barrier_finish_) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

// For testing only
void BasicMailbox::BindAndConnect() {
  context_ = zmq_ctx_new();
  CHECK(context_ != nullptr) << "create zmq context failed";
  zmq_ctx_set(context_, ZMQ_MAX_SOCKETS, 65536);

  Bind(scheduler_node_, 1);
  VLOG(2) << "Finished binding";
  Connect(scheduler_node_);
  VLOG(2) << "Finished connecting";
}

void BasicMailbox::CloseSockets() {
  int linger = 0;
  int rc = zmq_setsockopt(receiver_, ZMQ_LINGER, &linger, sizeof(linger));
  CHECK(rc == 0 || errno == ETERM);
  CHECK_EQ(zmq_close(receiver_), 0);
  for (auto &it : senders_) {
    int rc = zmq_setsockopt(it.second, ZMQ_LINGER, &linger, sizeof(linger));
    CHECK(rc == 0 || errno == ETERM);
    CHECK_EQ(zmq_close(it.second), 0);
  }
  zmq_ctx_destroy(context_);
}

void BasicMailbox::Connect(const Node &node) {
  CHECK_NE(node.id, node.kEmpty);
  CHECK_NE(node.port, node.kEmpty);
  auto it = senders_.find(node.id);
  if (it != senders_.end()) {
    zmq_close(it->second);
  }
  void *sender = zmq_socket(context_, ZMQ_DEALER);
  CHECK(sender != nullptr) << zmq_strerror(errno);
  if (my_node_.id != Node::kEmpty) {
    std::string my_id = "node" + std::to_string(my_node_.id);
    zmq_setsockopt(sender, ZMQ_IDENTITY, my_id.data(), my_id.size());
  }
  std::string addr = "tcp://" + node.hostname + ":" + std::to_string(node.port);
  if (zmq_connect(sender, addr.c_str()) != 0) {
    LOG(FATAL) << "connect to " + addr + " failed: " << zmq_strerror(errno);
  }
  senders_[node.id] = sender;
}

const Node &BasicMailbox::my_node() const {
  CHECK(ready_) << "call Start() first";
  return my_node_;
}

std::vector<Node> BasicMailbox::GetNodes() { return nodes_; }

void BasicMailbox::Bind(const Node &node, int max_retry) {
  receiver_ = zmq_socket(context_, ZMQ_ROUTER);
  CHECK(receiver_ != nullptr) << "create receiver socket failed: "
                              << zmq_strerror(errno);
  std::string address = "tcp://*:" + std::to_string(node.port);
  for (int i = 0; i < max_retry; i++) {
    if (zmq_bind(receiver_, address.c_str()) == 0)
      break;
    if (i == max_retry - 1)
      LOG(FATAL) << "bind to " + address + " failed: " << zmq_strerror(errno);
  }
}
} // namespace xyz
