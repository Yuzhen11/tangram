#pragma once

#include "base/actor.hpp"
#include "base/sarray_binstream.hpp"
#include "core/scheduler/control.hpp"
#include "core/plan/plan_spec.hpp"
#include "comm/abstract_sender.hpp"

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
  
  void SendMsgToScheduler(SArrayBinStream ctrl_bin, SArrayBinStream bin);
 private:
  std::shared_ptr<AbstractSender> sender_;
};

}  // namespace xyz


