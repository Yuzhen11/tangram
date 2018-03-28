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
#include "core/scheduler/checkpoint_manager.hpp"
#include "core/scheduler/recover_manager.hpp"
#include "core/scheduler/checkpoint_loader.hpp"
#include "core/scheduler/collection_status.hpp"

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
    // setup managers 
    checkpoint_loader_ = std::make_shared<CheckpointLoader>(elem_);
    collection_manager_ = std::make_shared<CollectionManager>(elem_);

    collection_status_ = std::make_shared<CollectionStatus>();

    block_manager_ = std::make_shared<BlockManager>(elem_, collection_manager_, builder);
    control_manager_ = std::make_shared<ControlManager>(elem_, 
            checkpoint_loader_, collection_status_);
    distribute_manager_ = std::make_shared<DistributeManager>(elem_, collection_manager_);
    write_manager_ = std::make_shared<WriteManager>(elem_);
    checkpoint_manager_ = std::make_shared<CheckpointManager>(elem_, checkpoint_loader_, collection_status_);
    recover_manager_ = std::make_shared<RecoverManager>(elem_, collection_manager_, checkpoint_loader_, collection_status_);
  }
  virtual ~Scheduler() override {
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
   * 2. TryRunPlan
   */
  virtual void Process(Message msg) override;

  // One worker register the program to scheduler
  void RegisterProgram(int, SArrayBinStream bin);

  void RunDummy();

  void Exit();

  void RunPlan(int plan_id);

private:
  std::shared_ptr<SchedulerElem> elem_;

  int register_program_count_ = 0;
  bool init_program_ = false;
  ProgramContext program_;

  std::promise<void> exit_promise_;
  bool start_ = false;

  std::shared_ptr<BlockManager> block_manager_;
  std::shared_ptr<ControlManager> control_manager_;
  std::shared_ptr<DistributeManager> distribute_manager_;
  std::shared_ptr<WriteManager> write_manager_;
  std::shared_ptr<CollectionManager> collection_manager_;
  std::shared_ptr<CheckpointManager> checkpoint_manager_;
  std::shared_ptr<RecoverManager> recover_manager_;
  std::shared_ptr<CheckpointLoader> checkpoint_loader_;
  
  std::chrono::system_clock::time_point start;
  std::chrono::system_clock::time_point end;

  std::unique_ptr<AbstractDagRunner> dag_runner_;
  std::string dag_runner_type_;

  std::shared_ptr<CollectionStatus> collection_status_;
};

} // namespace xyz
