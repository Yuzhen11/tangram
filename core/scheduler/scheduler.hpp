#pragma once

#include <future>
#include <atomic>

#include "base/actor.hpp"
#include "base/sarray_binstream.hpp"
#include "base/node.hpp"
#include "core/scheduler/control.hpp"
#include "core/program_context.hpp"
#include "comm/abstract_sender.hpp"

#include "core/scheduler/collection_view.hpp"
#include "core/program_context.hpp"

#include "io/assigner.hpp"

#include "glog/logging.h"

namespace xyz {

class Scheduler : public Actor {
 public:
  Scheduler(int qid, std::shared_ptr<AbstractSender> sender, std::shared_ptr<Assigner> assigner = nullptr): 
      Actor(qid), sender_(sender), assigner_(assigner) {
    Start();
  }
  virtual ~Scheduler() override {
    if (scheduler_thread_.joinable()) {
      scheduler_thread_.join();
    }
    Stop();
  }

  // make the scheduler ready and start receiving RegisterProgram
  void Ready(std::vector<Node> nodes) {
    LOG(INFO) << "[Scheduler] Ready";
    nodes_ = nodes;
    ready_ = true;
  }
  void Wait();

  void Run() {
    LOG(INFO) << "waiting for load";
    load_done_promise_.get_future().get();
    LOG(INFO) << "load finish";
    register_program_promise_.get_future().get();
    LOG(INFO) << "[Scheduler] Received all RegisterProgram, start InitWorkers";
    InitWorkers();
    init_worker_reply_promise_.get_future().get();
    LOG(INFO) << "[Scheduler] All workers registered, start scheduling.";
    StartScheduling();
  }

  /*
   * <- : receive
   * -> : send
   *
   * The initialization step includes:
   * 1. RegisterProgram <-
   * 2. InitWorkers ->
   * 3. InitWorkersReply <-
   * 4. StartScheduling
   */
  virtual void Process(Message msg) override;

  // One worker register the program to scheduler
  void RegisterProgram(SArrayBinStream bin);

  // Initialize all workers by sending the PartToNodeMap
  // and wait for replies.
  void InitWorkers();

  void InitWorkersReply(SArrayBinStream bin);

  void RunDummy();

  void Exit();

  // Send speculative command
  void RunSpeculativeMap();

  void StartScheduling();
  
  void SendToAllWorkers(ScheduleFlag flag, SArrayBinStream bin);

  void FinishBlock(SArrayBinStream bin);
 private:
  std::shared_ptr<AbstractSender> sender_;

  std::vector<int> workers_;
  int register_program_count_ = 0;
  int init_reply_count_ = 0;
  int num_workers_ = 0;   // TODO

  bool init_program_ = false;
  ProgramContext program_;
  std::unordered_map<int, CollectionView> collection_map_;

  std::shared_ptr<Assigner> assigner_;
  std::vector<Node> nodes_;

  std::promise<void> exit_promise_;

  std::atomic<bool> ready_{false};

  std::thread scheduler_thread_;
  std::promise<void> load_done_promise_;
  std::promise<void> register_program_promise_;
  std::promise<void> init_worker_reply_promise_;
};

}  // namespace xyz

