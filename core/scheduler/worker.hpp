#pragma once

#include "base/actor.hpp"
#include "base/sarray_binstream.hpp"
#include "core/scheduler/control.hpp"
#include "core/plan/plan_spec.hpp"
#include "comm/abstract_sender.hpp"
#include "core/plan/function_store.hpp"
#include "core/partition/partition_tracker.hpp"
#include "core/index/simple_part_to_node_mapper.hpp"

#include "io/loader.hpp"

#include "glog/logging.h"

namespace xyz {

class Worker : public Actor {
  Worker(int qid, std::shared_ptr<AbstractSender> sender): 
      Actor(qid), sender_(sender) {
    Start();
  }
  virtual ~Worker() override {
    Stop();
  }

  // Wait until the end signal.
  void Wait();

  virtual void Process(Message msg) override;

  // One worker register the plan to scheduler
  void RegisterPlan(PlanSpec plan);

  // Initialize all workers by sending the PartToNodeMap
  // and wait for replies.
  void InitWorkers(SArrayBinStream bin);

  // Run map on all workers
  void RunMap();

  // Send speculative command
  void RunSpeculativeMap();

  void LoadBlock(SArrayBinStream bin);
  
  void SendMsgToScheduler(SArrayBinStream ctrl_bin, SArrayBinStream bin);
 private:
  std::shared_ptr<AbstractSender> sender_;
  std::shared_ptr<PartitionTracker> partition_tracker_;
  std::shared_ptr<FunctionStore> function_store_;

  std::shared_ptr<HdfsLoader> loader_;

  // store the mapping from partition to node.
  std::unordered_map<int, std::shared_ptr<AbstractPartToNodeMapper>> part_to_node_map_;

};

}  // namespace xyz

