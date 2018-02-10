#pragma once

#include <thread>

#include "base/threadsafe_queue.hpp"
#include "base/message.hpp"
#include "base/sarray_binstream.hpp"

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
    Control ctrl;
    SArrayBinStream bin;
    ctrl.flag = Flag::kExit;
    bin << ctrl;
    msg.AddData(bin.ToSArray());
    work_queue_.Push(msg);
    work_thread_.join();
  }

 private:
  void Main() {
    while (true) {
      Message msg;
      work_queue_.WaitAndPop(&msg);
      Control ctrl;
      SArrayBinStream bin;
      bin.FromMsg(msg);
      bin >> ctrl;
      if (ctrl.flag == Flag::kExit) {
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

