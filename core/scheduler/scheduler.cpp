#include <algorithm>

#include "core/scheduler/scheduler.hpp"
#include "comm/simple_sender.hpp"
#include "core/queue_node_map.hpp"

namespace xyz {

void Scheduler::Process(Message msg) {
  CHECK_EQ(msg.data.size(), 2);  // cmd, content
  SArrayBinStream ctrl_bin, bin;
  ctrl_bin.FromSArray(msg.data[0]);
  bin.FromSArray(msg.data[1]);
  ScheduleFlag flag;
  ctrl_bin >> flag;
  switch (flag) {
    case ScheduleFlag::kRegisterProgram: {
      RegisterProgram(bin);
      break;
    }
    case ScheduleFlag::kInitWorkersReply: {
      InitWorkersReply(bin);
      break;
    }
    case ScheduleFlag::kFinishBlock: {
      FinishBlock(bin);
      break;
    }
    default: CHECK(false) << ScheduleFlagName[static_cast<int>(flag)];
  }
}

void Scheduler::RegisterProgram(SArrayBinStream bin) {
  if (!init_program_) {
    bin >> program_;
    LOG(INFO) << "[Scheduler] Receive program: " << program_.DebugString();
    CHECK_LE(program_.load_plans.size(), 1);
    if (program_.load_plans.size() == 1) {
      auto lp = program_.load_plans[0];
      std::vector<std::pair<std::string, int>> assigned_nodes(nodes_.size());
      std::transform(
        nodes_.begin(), nodes_.end(), assigned_nodes.begin(),
        [] (Node const& node){
        return std::make_pair(node.hostname, node.id);
        });  
      CHECK(assigner_);
      int num_blocks = assigner_->Load(lp.url, assigned_nodes, 1);
    } else {
      load_done_promise_.set_value();
    }
    for (auto c : program_.collections) {
      c.mapper.BuildRandomMap(c.num_partition, nodes_.size());  // Build the PartToNodeMap
      collection_map_.insert({c.collection_id, c});
    }
    init_program_ = true;
    // spawn the scheduler thread
    LOG(INFO) << "[Scheduler] starting the scheduling thread";
    scheduler_thread_ = std::thread([this]() { Run(); });
  }
  register_program_count_ += 1;
  if (register_program_count_ == nodes_.size()) {
    register_program_promise_.set_value();
  }
}

void Scheduler::InitWorkers() {
  LOG(INFO) << "[Scheduler] Initworker";
  // Send the collection_map_ to all workers.
  SArrayBinStream bin;
  bin << collection_map_;
  SArrayBinStream ctrl_bin;
  SendToAllWorkers(ScheduleFlag::kInitWorkers, bin);
}

void Scheduler::InitWorkersReply(SArrayBinStream bin) {
  init_reply_count_ += 1;
  if (init_reply_count_ == nodes_.size()) {
    init_worker_reply_promise_.set_value();
  }
}

void Scheduler::StartScheduling() {
  RunDummy();
  Exit();
}

void Scheduler::Exit() {
  LOG(INFO) << "[Scheduler] Exit";
  SArrayBinStream dummy_bin;
  SendToAllWorkers(ScheduleFlag::kExit, dummy_bin);
  exit_promise_.set_value();
}

void Scheduler::Wait() {
  LOG(INFO) << "[Scheduler] waiting";
  std::future<void> f = exit_promise_.get_future();
  f.get();
}

void Scheduler::RunDummy() {
  SArrayBinStream bin;
  SendToAllWorkers(ScheduleFlag::kDummy, bin);
}

void Scheduler::FinishBlock(SArrayBinStream bin) {
  FinishedBlock block;
  bin >> block;
  LOG(INFO) << "[Scheduler] FinishBlock";
  bool done = assigner_->FinishBlock(block);
  if (done) {
    // TODO
    auto blocks = assigner_->GetFinishedBlocks();
    // TODO: update collection map
    load_done_promise_.set_value();
  }
}


void Scheduler::SendToAllWorkers(ScheduleFlag flag, SArrayBinStream bin) {
  SArrayBinStream ctrl_bin;
  ctrl_bin << flag;
  for (auto node : nodes_) {
    Message msg;
    msg.meta.sender = 0;
    msg.meta.recver = GetWorkerQid(node.id);
    msg.meta.flag = Flag::kOthers;
    msg.AddData(ctrl_bin.ToSArray());
    msg.AddData(bin.ToSArray());
    sender_->Send(std::move(msg));
  }
}

}  // namespace xyz

