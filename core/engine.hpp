#pragma once

#include <map>
#include <memory>
#include <sstream>

#include "core/executor/executor.hpp"
#include "core/partition/partition_manager.hpp"
#include "core/map_output/abstract_map_output.hpp"
#include "core/plan/function_store.hpp"
#include "core/intermediate/intermediate_store.hpp"
#include "core/plan/plan_spec.hpp"

#include "core/scheduler/worker.hpp"
#include "core/engine_elem.hpp"
#include "core/join_actor.hpp"
#include "core/cache/fetcher.hpp"
#include "comm/worker_mailbox.hpp"
#include "comm/sender.hpp"

#include "io/hdfs_block_reader.hpp"
#include "io/hdfs_writer.hpp"

namespace xyz {

class Engine {
 public:
  struct Config {
    std::string scheduler;
    int scheduler_port;
    int num_threads;
    std::string namenode;
    int port;
    std::string DebugString() const {
      std::stringstream ss;
      ss << " { ";
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
  std::shared_ptr<JoinActor> join_actor_;
  std::shared_ptr<Fetcher> fetcher_;
  std::shared_ptr<Controller> controller_;
};

}  // namespace xyz

