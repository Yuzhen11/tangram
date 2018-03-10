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
#include "core/partition/partition_tracker.hpp"
#include "core/plan/function_store.hpp"
#include "core/plan/plan_spec.hpp"
#include "core/scheduler/control.hpp"

#include "core/program_context.hpp"

#include "glog/logging.h"
#include "io/block_reader_wrapper.hpp"
#include "io/writer_wrapper.hpp"
#include "core/worker/controller.hpp"

namespace xyz {

class Worker : public Actor {
public:
  Worker(int qid, EngineElem engine_elem, std::shared_ptr<BlockReaderWrapper> block_reader_wrapper,
         std::shared_ptr<WriterWrapper> writer,
         std::shared_ptr<Controller> controller)
      : Actor(qid), engine_elem_(engine_elem), block_reader_wrapper_(block_reader_wrapper),
        writer_(writer), controller_(controller) {
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

  // Process the kInitWorkers msg from scheduler
  // and send back reply
  void InitWorkers(SArrayBinStream bin);

  // Run map on this worker
  void RunMap(SArrayBinStream);
  void RunController(SArrayBinStream);

  void RunDummy();

  // Send speculative command
  void RunSpeculativeMap();

  void LoadBlock(SArrayBinStream bin);
  void Distribute(SArrayBinStream bin);
  void CheckPoint(SArrayBinStream bin);
  void WritePartition(SArrayBinStream bin);

  void SendMsgToScheduler(ScheduleFlag flag, SArrayBinStream bin);

  void Exit();
  void MapFinish();
  void JoinFinish();

private:
  int Id() { return engine_elem_.node.id; }
  std::string WorkerId() { 
    std::stringstream ss;
    ss << "[Worker " << Id() << "]: ";
    return ss.str();
  }
  EngineElem engine_elem_;
  std::shared_ptr<BlockReaderWrapper> block_reader_wrapper_;
  std::shared_ptr<WriterWrapper> writer_;
  std::shared_ptr<Controller> controller_;

  std::promise<void> exit_promise_;

  ProgramContext program_;
  bool is_program_set_ = false;

  bool ready_ = false;
};

} // namespace xyz
