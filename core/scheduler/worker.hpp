#pragma once

#include "base/actor.hpp"
#include "base/sarray_binstream.hpp"
#include "core/scheduler/control.hpp"
#include "core/plan/plan_spec.hpp"
#include "comm/abstract_sender.hpp"
#include "core/plan/function_store.hpp"
#include "core/partition/partition_tracker.hpp"
#include "core/index/simple_part_to_node_mapper.hpp"
#include "core/engine_elem.hpp"

#include "io/loader.hpp"
#include "io/hdfs_reader.hpp"

#include "glog/logging.h"

namespace xyz {

class Worker : public Actor {
 public:
  Worker(int qid, EngineElem engine_elem): 
      Actor(qid), engine_elem_(engine_elem) {
    auto reader = std::make_shared<HdfsReader>();
    loader_ = std::make_shared<HdfsLoader>(qid, engine_elem_.sender, reader, engine_elem_.executor,
            engine_elem_.partition_manager, engine_elem_.namenode, engine_elem_.port,
            engine_elem_.node);
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
  EngineElem engine_elem_;
  std::shared_ptr<HdfsLoader> loader_;

  // store the mapping from partition to node.
  std::unordered_map<int, std::shared_ptr<AbstractPartToNodeMapper>> part_to_node_map_;

};

}  // namespace xyz

