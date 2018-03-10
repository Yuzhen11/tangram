#pragma once

#include <atomic>
#include <future>
#include <chrono>

#include "core/scheduler/scheduler_elem.hpp"
#include "core/scheduler/block_manager.hpp"

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
            std::function<std::shared_ptr<Assigner>()> builder)
      : Actor(qid) {
    // setup elem_
    elem_ = std::make_shared<SchedulerElem>();
    elem_->sender = sender;
    elem_->collection_map = std::make_shared<CollectionMap>();
    // setup block_manager_
    block_manager_ = std::make_shared<BlockManager>(elem_, builder);
  }
  virtual ~Scheduler() override {
    if (scheduler_thread_.joinable()) {
      scheduler_thread_.join();
    }
    if (start_) {
      Stop();
    }
  }

  void Wait();

  // make the scheduler ready and start receiving RegisterProgram
  void Ready(std::vector<Node> nodes);

  void Run() {
    spec_count_ = -1;
    RunNextSpec();
  }

  /*
   * <- : receive
   * -> : send
   *
   * The initialization step includes:
   * 1. RegisterProgram <-
   * 2. InitWorkers ->
   * 3. InitWorkersReply <-
   * 4. StartScheduling
   */
  virtual void Process(Message msg) override;

  // One worker register the program to scheduler
  void RegisterProgram(int, SArrayBinStream bin);

  // Initialize all workers by sending the PartToNodeMap
  // and wait for replies.
  void InitWorkers();

  void InitWorkersReply(SArrayBinStream bin);

  void RunDummy();

  void RunMap();

  void Exit();

  // Send speculative command
  void RunSpeculativeMap();

  void StartScheduling();

  void SendToAllWorkers(ScheduleFlag flag, SArrayBinStream bin);

  // void FinishBlock(SArrayBinStream bin);
  void FinishDistribute(SArrayBinStream bin);
  void CheckPoint();
  void Write(SpecWrapper s);
  void FinishCheckPoint(SArrayBinStream bin);
  void FinishWritePartition(SArrayBinStream bin);
  void FinishJoin(SArrayBinStream bin);

  void SendTo(int node_id, ScheduleFlag flag, SArrayBinStream bin);

private:
  void Load(LoadSpec* spec);
  void Distribute(DistributeSpec* spec);
  // void TryRunPlan();
  void PrepareNextCollection();

  void RunNextSpec();
  void RunNextIteration();
private:
  std::shared_ptr<SchedulerElem> elem_;

  // std::shared_ptr<AbstractSender> sender_;

  int register_program_count_ = 0;
  int init_reply_count_ = 0;

  bool init_program_ = false;
  ProgramContext program_;
  // std::unordered_map<int, CollectionView> collection_map_;

  // std::shared_ptr<Assigner> assigner_;
  // std::vector<Node> nodes_;
  // std::vector<int> num_local_threads_;
  // std::map<int, NodeInfo> nodes_;

  std::promise<void> exit_promise_;

  bool start_ = false;

  std::thread scheduler_thread_;

  int distribute_part_expected_ = 0;

  // collection_id, part_id, node_id
  std::map<int, std::map<int, int>> distribute_map_;

  int num_workers_finish_a_plan_iteration_ = 0;
  int cur_iters_ = 0;

  int spec_count_ = -1;
  SpecWrapper currnet_spec_;

  int write_reply_count_ = 0;
  int expected_write_reply_count_;

  std::shared_ptr<BlockManager> block_manager_;
  
  std::chrono::system_clock::time_point start;
  std::chrono::system_clock::time_point end;
};

} // namespace xyz
