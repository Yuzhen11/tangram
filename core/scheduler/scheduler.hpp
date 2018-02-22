#pragma once

#include "base/actor.hpp"
#include "base/sarray_binstream.hpp"
#include "core/scheduler/control.hpp"
#include "core/plan/plan_spec.hpp"
#include "comm/abstract_sender.hpp"

#include "core/scheduler/collection_view.hpp"

#include "io/assigner.hpp"

#include "glog/logging.h"

namespace xyz {

class Scheduler : public Actor {
  Scheduler(int qid, std::shared_ptr<AbstractSender> sender): 
      Actor(qid), sender_(sender) {
    Start();
  }
  virtual ~Scheduler() override {
    Stop();
  }

  /*
   * <- : receive
   * -> : send
   *
   * The initialization step includes:
   * 1. RegisterPlan <-
   * 2. InitWorkers ->
   * 3. InitWorkersReply <-
   * 4. StartScheduling
   */
  virtual void Process(Message msg) override;

  // One worker register the plan to scheduler
  void RegisterPlan(SArrayBinStream bin);

  // Initialize all workers by sending the PartToNodeMap
  // and wait for replies.
  void InitWorkers();

  void InitWorkersReply(SArrayBinStream bin);

  // Run map on all workers
  void RunMap();

  // Send speculative command
  void RunSpeculativeMap();

  void StartScheduling();
  
  void SendToAllWorkers(SArrayBinStream ctrl_bin, SArrayBinStream bin);

  void FinishBlock(SArrayBinStream bin);
 private:
  std::shared_ptr<AbstractSender> sender_;

  std::vector<int> workers_;
  int init_reply_count_ = 0;
  int num_workers_ = 0;   // TODO

  PlanSpec plan_spec_;  // The plan that it is going to be run.
  std::unordered_map<int, CollectionView> collection_map_;

  std::shared_ptr<Assigner> assigner_;
};

}  // namespace xyz

