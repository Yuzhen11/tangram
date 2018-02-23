#include "core/scheduler/scheduler.hpp"

namespace xyz {

void Scheduler::StartCluster() {
  SArrayBinStream bin;
  SendToAllWorkers(ScheduleFlag::kStart, bin);
}

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

    for (auto c : program_.collections) {
      c.mapper.BuildRandomMap(c.num_partition, nodes_.size());  // Build the PartToNodeMap
      collection_map_.insert({c.collection_id, c});
    }
    InitWorkers();
  }
}

void Scheduler::InitWorkers() {
  // Send the collection_map_ to all workers.
  SArrayBinStream bin;
  bin << collection_map_;
  SArrayBinStream ctrl_bin;
  SendToAllWorkers(ScheduleFlag::kInitWorkers, bin);
}

void Scheduler::InitWorkersReply(SArrayBinStream bin) {
  init_reply_count_ += 1;
  if (init_reply_count_ == num_workers_) {
    LOG(INFO) << "All workers registered, start scheduling.";
    StartScheduling();
  }
}

void Scheduler::StartScheduling() {
  RunMap();
}

void Scheduler::RunMap() {
  SArrayBinStream bin;
  SendToAllWorkers(ScheduleFlag::kRunMap, bin);
}

void Scheduler::FinishBlock(SArrayBinStream bin) {
  FinishedBlock block;
  bin >> block;
  assigner_->FinishBlock(block);
}


void Scheduler::SendToAllWorkers(ScheduleFlag flag, SArrayBinStream bin) {
  SArrayBinStream ctrl_bin;
  ctrl_bin << flag;
  for (auto node : nodes_) {
    Message msg;
    msg.meta.sender = 0;
    msg.meta.recver = node.id * 10;
    msg.meta.flag = Flag::kOthers;
    msg.AddData(ctrl_bin.ToSArray());
    msg.AddData(bin.ToSArray());
    sender_->Send(std::move(msg));
  }
}

}  // namespace xyz

