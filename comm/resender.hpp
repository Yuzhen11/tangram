#ifndef RESENDER_H_
#define RESENDER_H_
#include <chrono>
#include <vector>
#include <unordered_set>
#include <unordered_map>

#include <comm/mailbox.hpp>
namespace xyz {

// resend a messsage if no ack is received within a given time
class Resender {
 public:
  // timeout timeout in millisecond
  Resender(int timeout, int max_retry, Mailbox* mailbox) {
    timeout_ = timeout;
    max_retry_ = max_retry;
    mailbox_ = mailbox;
    monitor_ = new std::thread(&Resender::Monitoring, this);
  }
  ~Resender() {
    exit_ = true;
    monitor_->join();
    delete monitor_;
  }

  // add an incomming message, return true if msg has been added before or a ACK message
  bool AddIncomming(const Message& msg) {
  }

 private:
  void Monitoring() {
    // TODO
  }

  std::thread* monitor_;
  std::unordered_set<uint64_t> acked_;
  std::atomic<bool> exit_{false};
  std::mutex mu_;
  int timeout_;
  int max_retry_;
  Mailbox* mailbox_;
};
}  // namespace xyz
#endif  // RESENDER_H_
