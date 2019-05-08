#pragma once

#include <map>
#include <memory>
#include <sstream>
#include <iterator>

#include "core/executor/executor.hpp"
#include "core/partition/partition_manager.hpp"
#include "core/map_output/abstract_map_output.hpp"
#include "core/plan/function_store.hpp"
#include "core/intermediate/intermediate_store.hpp"
#include "core/plan/plan_spec.hpp"

#include "core/scheduler/worker.hpp"
#include "core/engine_elem.hpp"
#include "core/cache/fetcher.hpp"
#include "comm/worker_mailbox.hpp"
#include "comm/sender.hpp"

#include "core/worker/controller.hpp"

namespace xyz {

class Engine {
 public:
  struct Config {
    std::string scheduler;
    int scheduler_port;
    int num_local_threads;
    int num_update_threads;
    int num_combine_threads;
    std::string namenode;
    int port;
    std::string DebugString() const {
      std::stringstream ss;
      ss << " { ";
      ss << ", scheduler: " << scheduler;
      ss << ", scheduler_port: " << scheduler_port;
      ss << ", num_local_threads: " << num_local_threads;
      ss << ", num_update_threads: " << num_update_threads;
      ss << ", num_combine_threads: " << num_combine_threads;
      ss << ", namenode: " << namenode;
      ss << ", port: " << port;
      ss << " } ";
      return ss.str();
    }
  };

  Engine() = default;
  ~Engine() = default;

  void RegisterProgram(ProgramContext program) {
    program_ = program;
  }
  void Init(Engine::Config config);
  void Start();
  void Run();
  void Stop();

  template <typename Plan>
  void AddFunc(Plan plan) {
    plan.Register(engine_elem_.function_store);
  }

  template <typename Plan>
  void AddFunc(Plan* plan) {
    plan->Register(engine_elem_.function_store);
  }

 private:
  ProgramContext program_;
  EngineElem engine_elem_;
  Config config_;

  std::shared_ptr<WorkerMailbox> mailbox_;
  std::shared_ptr<Worker> worker_;
  std::shared_ptr<Fetcher> fetcher_;
  std::shared_ptr<Controller> controller_;
};

}  // namespace xyz

