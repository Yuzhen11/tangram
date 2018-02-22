#include "core/scheduler/scheduler.hpp"

namespace xyz {

void Scheduler::Process(Message msg) {
  CHECK_EQ(msg.data.size(), 2);  // cmd, content
  SArrayBinStream ctrl_bin;
  SArrayBinStream bin;
  ctrl_bin.FromSArray(msg.data[0]);
  bin.FromSArray(msg.data[1]);
  ScheduleFlag flag;
  ctrl_bin >> flag;
  switch (flag) {
    case ScheduleFlag::kRegisterPlan: {
      RegisterPlan(bin);
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

void Scheduler::RegisterPlan(SArrayBinStream bin) {
  // JobSpec
  bin >> plan_spec_;
  // CollectionView
  int num_collection;
  bin >> num_collection;
  for (int i = 0; i < num_collection; ++ i) {
    CollectionView c;
    bin >> c;
    c.mapper.BuildRandomMap(c.num_partition, num_workers_);  // Build the PartToNodeMap
    collection_map_.insert({c.collection_id, c});
  }
  InitWorkers();
}

void Scheduler::InitWorkers() {
  // Send the collection_map_ to all workers.
  SArrayBinStream bin;
  bin << int(collection_map_.size());
  for (auto& kv : collection_map_) {
    bin << kv.second;
  }
  SArrayBinStream ctrl_bin;
  ctrl_bin << ScheduleFlag::kInitWorkers;
  SendToAllWorkers(ctrl_bin, bin);
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
  SArrayBinStream ctrl_bin;
  ctrl_bin << ScheduleFlag::kRunMap;
  SArrayBinStream bin;
  SendToAllWorkers(ctrl_bin, bin);
}

void Scheduler::SendToAllWorkers(SArrayBinStream ctrl_bin, SArrayBinStream bin) {
  for (auto w : workers_) {
    Message msg;
    // TODO fill the meta
    msg.AddData(ctrl_bin.ToSArray());
    msg.AddData(bin.ToSArray());
    sender_->Send(std::move(msg));
  }
}

void Scheduler::FinishBlock(SArrayBinStream bin) {
  FinishedBlock block;
  bin >> block;
  assigner_->FinishBlock(block);
}

}  // namespace xyz

