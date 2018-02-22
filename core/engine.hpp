#pragma once

#include <map>
#include <memory>
#include <sstream>

#include "core/executor/executor.hpp"
#include "core/partition/partition_manager.hpp"
#include "core/map_output/abstract_map_output.hpp"
#include "core/plan/function_store.hpp"
#include "core/intermediate/simple_intermediate_store.hpp"
#include "core/plan/plan_spec.hpp"

#include "core/scheduler/worker.hpp"

namespace xyz {

class Engine {
 public:
  struct Config {
    int num_workers;
    std::string scheduler;
    int scheduler_port;
    int num_threads;
    std::string DebugString() const {
      std::stringstream ss;
      ss << " { ";
      ss << "num workers: " << num_workers;
      ss << ", scheduler: " << scheduler;
      ss << ", scheduler_port: " << scheduler_port;
      ss << ", num_threads: " << num_threads;
      ss << " } ";
      return ss.str();
    }
  };

  Engine();
  ~Engine();

  void Start();
  void Run();
  void Stop();

  template <typename Plan>
  void Add(Plan plan) {
    plan.Register(function_store_);
    PlanSpec plan_spec = plan.GetPlanSpec();
  }

 private:
  std::shared_ptr<Executor> executor_;
  std::shared_ptr<PartitionManager> partition_manager_;
  std::shared_ptr<FunctionStore> function_store_;
  std::shared_ptr<AbstractIntermediateStore> intermediate_store_;
  std::shared_ptr<PartitionTracker> partition_tracker_;

  std::shared_ptr<Mailbox> mailbox_;
  std::shared_ptr<AbstractSender> sender_;
  std::shared_ptr<Worker> worker_;
  std::shared_ptr<JoinActor> join_actor_;
};

}  // namespace

