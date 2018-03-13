#pragma once

#include <atomic>
#include <future>
#include <chrono>

#include "core/scheduler/scheduler_elem.hpp"
#include "core/scheduler/block_manager.hpp"
#include "core/scheduler/control_manager.hpp"
#include "core/scheduler/write_manager.hpp"
#include "core/scheduler/distribute_manager.hpp"
#include "core/scheduler/collection_manager.hpp"

#include "core/scheduler/dag_runner.hpp"

#include "base/actor.hpp"
#include "base/node.hpp"
#include "base/sarray_binstream.hpp"
#include "comm/abstract_sender.hpp"
#include "core/program_context.hpp"
#include "core/scheduler/control.hpp"

#include "core/program_context.hpp"
#include "core/scheduler/collection_view.hpp"

#include "io/assigner.hpp"
#include "io/meta.hpp"

#include "glog/logging.h"

namespace xyz {

class Scheduler : public Actor {
public:
  Scheduler(int qid, std::shared_ptr<AbstractSender> sender,
            std::function<std::shared_ptr<Assigner>()> builder,
            std::string dag_runner_type)
      : Actor(qid), dag_runner_type_(dag_runner_type) {
    CHECK(dag_runner_type_ == "sequential"
       || dag_runner_type_ == "wide");
    // setup elem_
    elem_ = std::make_shared<SchedulerElem>();
    elem_->sender = sender;
    elem_->collection_map = std::make_shared<CollectionMap>();
    // setup block_manager_
    block_manager_ = std::make_shared<BlockManager>(elem_, builder);
    control_manager_ = std::make_shared<ControlManager>(elem_);
    distribute_manager_ = std::make_shared<DistributeManager>(elem_);
    write_manager_ = std::make_shared<WriteManager>(elem_);
    collection_manager_ = std::make_shared<CollectionManager>(elem_);
  }
  virtual ~Scheduler() override {
    // if (scheduler_thread_.joinable()) {
    //   scheduler_thread_.join();
    // }
    if (start_) {
      Stop();
    }
  }

  void Wait();

  // make the scheduler ready and start receiving RegisterProgram
  void Ready(std::vector<Node> nodes);

  void TryRunPlan();
  /*
   * <- : receive
   * -> : send
   *
   * The initialization step includes:
   * 1. RegisterProgram <-
   * 2. StartScheduling
   */
  virtual void Process(Message msg) override;

  // One worker register the program to scheduler
  void RegisterProgram(int, SArrayBinStream bin);

  void RunDummy();

  // void RunMap();

  void Exit();

  // Send speculative command
  void RunSpeculativeMap();

  void Checkpoint(SpecWrapper s);
  void LoadCheckpoint(SpecWrapper s);
  void FinishCheckPoint(SArrayBinStream bin);
  void FinishLoadCheckPoint(SArrayBinStream bin);
  // void FinishJoin(SArrayBinStream bin);

  void RunPlan(int plan_id);

private:
  // void TryRunPlan();
  void PrepareNextCollection();

  // void RunNextSpec();
  // void RunNextIteration();
private:
  std::shared_ptr<SchedulerElem> elem_;

  int register_program_count_ = 0;
  bool init_program_ = false;
  ProgramContext program_;

  std::promise<void> exit_promise_;
  bool start_ = false;
  // std::thread scheduler_thread_;

  // int spec_count_ = 0;
  // SpecWrapper currnet_spec_;
  int num_workers_finish_a_plan_iteration_ = 0;
  int cur_iters_ = 0;

  std::shared_ptr<BlockManager> block_manager_;
  std::shared_ptr<ControlManager> control_manager_;
  std::shared_ptr<DistributeManager> distribute_manager_;
  std::shared_ptr<WriteManager> write_manager_;
  std::shared_ptr<CollectionManager> collection_manager_;
  
  std::chrono::system_clock::time_point start;
  std::chrono::system_clock::time_point end;

  std::unique_ptr<AbstractDagRunner> dag_runner_;
  std::string dag_runner_type_;
};

} // namespace xyz
