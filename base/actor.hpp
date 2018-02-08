#pragma once

#include "base/threadsafe_queue.hpp"
#include "base/message.hpp"

namespace xyz {

class Actor {
 public:
  Actor(int qid): queue_id_(qid) {}
  virtual ~Actor() = default;

  ThreadsafeQueue<Message>* GetWorkQueue() { return &work_queue_; }

  virtual void Process(Message msg) = 0;

  void Start() {
    work_thread_ = std::thread([this]() {
      Main();
    });
  }

  void Stop() {
    Message msg;
    msg.meta.flag = Flag::kExit;
    work_queue_.Push(msg);
    work_thread_.join();
  }

 private:
  void Main() {
    while (true) {
      Message msg;
      work_queue_.WaitAndPop(&msg);
      if (msg.meta.flag == Flag::kExit) {
        break;
      }
      Process(std::move(msg));
    }
  }

 private:
  int queue_id_;
  ThreadsafeQueue<Message> work_queue_;
  std::thread work_thread_;
};

}  // namespace xyz

