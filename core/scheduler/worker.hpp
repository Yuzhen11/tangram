#pragma once

#include <atomic>
#include <chrono>
#include <future>
#include <thread>

#include "base/actor.hpp"
#include "base/sarray_binstream.hpp"
#include "comm/abstract_sender.hpp"
#include "core/engine_elem.hpp"
#include "core/index/simple_part_to_node_mapper.hpp"
#include "core/plan/function_store.hpp"
#include "core/plan/plan_spec.hpp"

#include "core/program_context.hpp"

#include "glog/logging.h"
#include "io/io_wrapper.hpp"
#include "io/io_wrapper.hpp"

namespace xyz {

class Worker : public Actor {
public:
  Worker(int qid, EngineElem engine_elem,
         std::shared_ptr<IOWrapper> io_wrapper,
         std::function<std::shared_ptr<AbstractBlockReader>()> block_reader_getter)
      : Actor(qid), engine_elem_(engine_elem),
        io_wrapper_(io_wrapper),
        block_reader_getter_(block_reader_getter){
    Start();
  }
  virtual ~Worker() override { Stop(); }

  // public api:
  // SetProgram should be called before kStart is recevied.
  void SetProgram(ProgramContext program) {
    program_ = program;
    is_program_set_ = true;
  }

  void RegisterProgram();

  // Wait until the end signal.
  void Wait();

  virtual void Process(Message msg) override;

  // The scheduler requests program from workers.
  void StartCluster();

  void UpdateCollection(SArrayBinStream bin);

  void RunDummy();

  void LoadBlock(SArrayBinStream bin);
  void Distribute(SArrayBinStream bin);
  void CheckPoint(SArrayBinStream bin);
  void LoadCheckPoint(SArrayBinStream bin);
  void WritePartition(SArrayBinStream bin);

  void SendMsgToScheduler(ScheduleFlag flag, SArrayBinStream bin);

  void Exit();
private:
  int Id() { return engine_elem_.node.id; }
  std::string WorkerId() { 
    std::stringstream ss;
    ss << "[Worker " << Id() << "]: ";
    return ss.str();
  }
  EngineElem engine_elem_;
  std::shared_ptr<IOWrapper> io_wrapper_;
  std::function<std::shared_ptr<AbstractBlockReader>()> block_reader_getter_;

  std::promise<void> exit_promise_;

  ProgramContext program_;
  bool is_program_set_ = false;

  bool ready_ = false;
};

} // namespace xyz
