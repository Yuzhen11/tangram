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
#include "core/engine_elem.hpp"
#include "core/join_actor.hpp"
#include "comm/mailbox.hpp"
#include "comm/sender.hpp"

#include "io/hdfs_reader.hpp"

namespace xyz {

class Engine {
 public:
  struct Config {
    int num_workers;
    std::string scheduler;
    int scheduler_port;
    int num_threads;
    std::string namenode;
    int port;
    std::string DebugString() const {
      std::stringstream ss;
      ss << " { ";
      ss << "num workers: " << num_workers;
      ss << ", scheduler: " << scheduler;
      ss << ", scheduler_port: " << scheduler_port;
      ss << ", num_threads: " << num_threads;
      ss << ", namenode: " << namenode;
      ss << ", port: " << port;
      ss << " } ";
      return ss.str();
    }
  };

  Engine() = default;
  ~Engine() = default;

  void Start(Engine::Config config);
  void Run();
  void Stop();

  template <typename Plan>
  void Add(Plan plan) {
    plan.Register(engine_elem_.function_store);
    PlanSpec plan_spec = plan.GetPlanSpec();
  }

 private:
  EngineElem engine_elem_;

  std::shared_ptr<Mailbox> mailbox_;
  std::shared_ptr<Worker> worker_;
  std::shared_ptr<JoinActor> join_actor_;
};

}  // namespace xyz

