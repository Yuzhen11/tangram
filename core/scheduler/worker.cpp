#include "core/scheduler/worker.hpp"
#include "core/plan/collection_spec.hpp"
#include "core/queue_node_map.hpp"
#include "core/shuffle_meta.hpp"

namespace xyz {

void Worker::Wait() {
  std::future<void> f = exit_promise_.get_future();
  f.get();
}

void Worker::RegisterProgram() {
  LOG(INFO) << "[Worker] RegisterProgram";
  CHECK(is_program_set_);
  ready_ = true;

  SArrayBinStream bin;
  bin << program_;
  SendMsgToScheduler(ScheduleFlag::kRegisterProgram, bin);
}

void Worker::Process(Message msg) {
  CHECK_EQ(msg.data.size(), 2); // cmd, content
  SArrayBinStream ctrl_bin;
  SArrayBinStream bin;
  ctrl_bin.FromSArray(msg.data[0]);
  bin.FromSArray(msg.data[1]);
  ScheduleFlag flag;
  ctrl_bin >> flag;

  switch (flag) {
  case ScheduleFlag::kInitWorkers: {
    InitWorkers(bin);
    break;
  }
  case ScheduleFlag::kRunMap: {
    RunMap(bin);
    break;
  }
  case ScheduleFlag::kLoadBlock: {
    LoadBlock(bin);
    break;
  }
  case ScheduleFlag::kDummy: {
    RunDummy();
    break;
  }
  case ScheduleFlag::kExit: {
    Exit();
    break;
  }
  case ScheduleFlag::kMapFinish: {
    MapFinish();
    break;
  }
  case ScheduleFlag::kJoinFinish: {
    JoinFinish();
    break;
  }
  case ScheduleFlag::kDistribute: {
    Distribute(bin);
    break;
  }
  case ScheduleFlag::kCheckPoint: {
    CheckPoint(bin);
    break;
  }
  default:
    CHECK(false);
  }
}

void Worker::InitWorkers(SArrayBinStream bin) {
  std::unordered_map<int, CollectionView> collection_map;
  bin >> collection_map;
  LOG(INFO) << "[Worker] collection map size: " << collection_map.size();
  for (auto kv : collection_map) {
    LOG(INFO) << kv.first << " " << kv.second.DebugString();
  }
  engine_elem_.collection_map->Init(collection_map);
  SArrayBinStream dummy_bin;
  SendMsgToScheduler(ScheduleFlag::kInitWorkersReply, dummy_bin);
}

void Worker::RunDummy() { LOG(INFO) << "[Worker] RunDummy"; }

void Worker::RunMap(SArrayBinStream bin) {
  LOG(INFO) << "[Worker] [qid " << Qid() << "] RunMap";
  int plan_id;
  bin >> plan_id;
  auto func = engine_elem_.function_store->GetMap(plan_id);
  PlanSpec plan = plan_map_[plan_id];
  engine_elem_.partition_tracker->SetPlan(
      plan); // set plan before run partition tracker
  engine_elem_.partition_tracker->RunAllMap(
      [func, this](ShuffleMeta meta, std::shared_ptr<AbstractPartition> p,
                   std::shared_ptr<AbstractMapProgressTracker> pt) {
        func(meta, p, engine_elem_.intermediate_store, pt);
      });
}

void Worker::LoadBlock(SArrayBinStream bin) {
  LOG(INFO) << "[Worker] LoadBlock";
  AssignedBlock block;
  bin >> block;
  loader_->Load(block, 
  engine_elem_.function_store->GetCreatePartFromReader(block.collection_id),
  [this](SArrayBinStream bin) {
    SendMsgToScheduler(ScheduleFlag::kFinishBlock, bin);
  }
  );
}

void Worker::Distribute(SArrayBinStream bin) {
  LOG(INFO) << "[Worker] Distribute";
  int part_id;
  CollectionSpec spec;
  bin >> part_id >> spec;
  auto func = engine_elem_.function_store->GetCreatePartFromBin(spec.collection_id);
  auto part = func(spec.data, part_id, spec.num_partition);
  engine_elem_.partition_manager->Insert(spec.collection_id, part_id, std::move(part));
  SArrayBinStream reply_bin;
  reply_bin << spec.collection_id << part_id << engine_elem_.node.id;
  SendMsgToScheduler(ScheduleFlag::kFinishDistribute, reply_bin);
}

void Worker::CheckPoint(SArrayBinStream bin) {
  int collection_id, part_id;
  std::string dest_url;
  bin >> collection_id >> part_id >> dest_url;

  writer_->Write(collection_id, part_id, dest_url, 
    [](std::shared_ptr<AbstractPartition> p) { 
       SArrayBinStream bin;
       p->ToBin(bin);
       return bin;
    }, 
    [this](SArrayBinStream bin) {
      SendMsgToScheduler(ScheduleFlag::kFinishCheckPoint, bin);
    }
  );
}

void Worker::Exit() { exit_promise_.set_value(); }
void Worker::MapFinish() {
  LOG(INFO) << "[Worker] [qid " << Qid() << " ]"
            << " MapFinish";
}
void Worker::JoinFinish() {
  LOG(INFO) << "[Worker] [qid " << Qid() << " ]"
            << " JoinFinish";
  SArrayBinStream bin;
  SendMsgToScheduler(ScheduleFlag::kJoinFinish, bin);
}

void Worker::SendMsgToScheduler(ScheduleFlag flag, SArrayBinStream bin) {
  Message msg;
  msg.meta.sender = GetWorkerQid(engine_elem_.node.id);
  msg.meta.recver = 0;
  msg.meta.flag = Flag::kOthers;
  SArrayBinStream ctrl_bin;
  ctrl_bin << flag;
  msg.AddData(ctrl_bin.ToSArray());
  msg.AddData(bin.ToSArray());
  engine_elem_.sender->Send(std::move(msg));
}

} // namespace xyz
