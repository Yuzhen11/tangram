#pragma once

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
  Scheduler(int qid, std::shared_ptr<AbstractSender> sender, std::vector<Node> nodes): 
      Actor(qid), sender_(sender), nodes_(nodes) {
    Start();
  }
  virtual ~Scheduler() override {
    Stop();
  }

  // Tell all workers to start
  void StartCluster();

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

  // Run map on all workers
  void RunMap();

  // Send speculative command
  void RunSpeculativeMap();

  void StartScheduling();
  
  void SendToAllWorkers(ScheduleFlag flag, SArrayBinStream bin);

  void FinishBlock(SArrayBinStream bin);
 private:
  std::shared_ptr<AbstractSender> sender_;

  std::vector<int> workers_;
  int init_reply_count_ = 0;
  int num_workers_ = 0;   // TODO

  bool init_program_ = false;
  ProgramContext program_;
  std::unordered_map<int, CollectionView> collection_map_;

  std::shared_ptr<Assigner> assigner_;
  std::vector<Node> nodes_;
};

}  // namespace xyz

