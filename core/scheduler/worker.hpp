#pragma once

#include <future>
#include <chrono>
#include <thread>
#include <atomic>

#include "base/actor.hpp"
#include "base/sarray_binstream.hpp"
#include "core/scheduler/control.hpp"
#include "core/plan/plan_spec.hpp"
#include "comm/abstract_sender.hpp"
#include "core/plan/function_store.hpp"
#include "core/partition/partition_tracker.hpp"
#include "core/index/simple_part_to_node_mapper.hpp"
#include "core/engine_elem.hpp"

#include "core/program_context.hpp"

#include "io/loader.hpp"

#include "glog/logging.h"

namespace xyz {

class Worker : public Actor {
 public:
  Worker(int qid, EngineElem engine_elem, std::shared_ptr<AbstractReader> reader): 
      Actor(qid), engine_elem_(engine_elem) {
    loader_ = std::make_shared<Loader>(qid, engine_elem_.sender, reader, engine_elem_.executor,
            engine_elem_.partition_manager, engine_elem_.namenode, engine_elem_.port,
            engine_elem_.node);
    Start();
  }
  virtual ~Worker() override {
    Stop();
  }

  // public api: 
  // SetProgram should be called before kStart is recevied.
  void SetProgram(ProgramContext program) {
    program_ = program;
    for (auto plan : program.plans) {
      plan_map_.insert({plan.plan_id, plan});
    }
    is_program_set_ = true;
  }

  void RegisterProgram();

  // Wait until the end signal.
  void Wait();

  virtual void Process(Message msg) override;

  // The scheduler requests program from workers.
  void StartCluster();

  // Process the kInitWorkers msg from scheduler
  // and send back reply
  void InitWorkers(SArrayBinStream bin);

  // Run map on this worker
  void RunMap(SArrayBinStream);

  void RunDummy();

  // Send speculative command
  void RunSpeculativeMap();

  void LoadBlock(SArrayBinStream bin);
  
  void SendMsgToScheduler(ScheduleFlag flag, SArrayBinStream bin);

  void Exit();
 private:
  EngineElem engine_elem_;
  std::shared_ptr<Loader> loader_;

  // store the mapping from partition to node.
  std::unordered_map<int, PlanSpec> plan_map_;

  std::promise<void> exit_promise_;

  ProgramContext program_;
  bool is_program_set_ = false;

  bool ready_ = false;
};

}  // namespace xyz

