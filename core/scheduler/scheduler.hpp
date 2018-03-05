#pragma once

#include <atomic>
#include <future>

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
            std::shared_ptr<Assigner> assigner = nullptr)
      : Actor(qid), sender_(sender), assigner_(assigner) {}
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
    // TODO do we need to lock some functions as two threads may work on the
    // same data.
    prepare_collection_count_ = -1;
    PrepareNextCollection();
    prepare_collection_promise_.get_future().get();
    LOG(INFO) << "[Scheduler] Finish collections preparation";

    InitWorkers();
    init_worker_reply_promise_.get_future().get();
    LOG(INFO) << "[Scheduler] Finish initiating workers, start scheduling.";
    StartScheduling();
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

  void RunMap(PlanSpec plan);

  void Exit();

  // Send speculative command
  void RunSpeculativeMap();

  void StartScheduling();

  void SendToAllWorkers(ScheduleFlag flag, SArrayBinStream bin);

  void FinishBlock(SArrayBinStream bin);
  void FinishDistribute(SArrayBinStream bin);
  void CheckPoint();
  void FinishCheckPoint(SArrayBinStream bin);
  void WritePartition();
  void FinishWritePartition(SArrayBinStream bin);
  void FinishJoin(SArrayBinStream bin);

private:
  void Load(CollectionSpec);
  void Distribute(CollectionSpec);
  void TryRunPlan();
  void PrepareNextCollection();

private:
  std::shared_ptr<AbstractSender> sender_;

  int register_program_count_ = 0;
  int init_reply_count_ = 0;

  bool init_program_ = false;
  ProgramContext program_;
  std::unordered_map<int, CollectionView> collection_map_;

  std::shared_ptr<Assigner> assigner_;
  // std::vector<Node> nodes_;
  // std::vector<int> num_local_threads_;
  struct NodeInfo {
    Node node;
    int num_local_threads;
  };
  std::map<int, NodeInfo> nodes_;

  std::promise<void> exit_promise_;

  bool start_ = false;

  std::thread scheduler_thread_;
  std::promise<void> init_worker_reply_promise_;

  int prepare_collection_count_ = 0;
  std::promise<void> prepare_collection_promise_;
  int distribute_part_expected_ = 0;

  // collection_id, part_id, node_id
  std::map<int, std::map<int, int>> distribute_map_;
  // collection_id, part_id, <url, offset, node_id>
  std::map<int, std::map<int, StoredBlock>> stored_blocks_;

  int num_workers_finish_a_plan_iteration_ = 0;
  int num_plan_iteration_finished_ = 0;
  int program_num_plans_finished_ = 0;
};

} // namespace xyz
