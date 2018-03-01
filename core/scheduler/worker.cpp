#include "core/scheduler/worker.hpp"
#include "core/plan/plan_spec.hpp"
#include "core/shuffle_meta.hpp"
#include "core/queue_node_map.hpp"

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
  CHECK_EQ(msg.data.size(), 2);  // cmd, content
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
    default: CHECK(false);
  }
}

void Worker::InitWorkers(SArrayBinStream bin) {
  std::unordered_map<int, CollectionView> collection_map;
  bin >> collection_map;
  engine_elem_.collection_map->Init(collection_map);
  SArrayBinStream dummy_bin;
  SendMsgToScheduler(ScheduleFlag::kInitWorkersReply, dummy_bin);
}

void Worker::RunDummy() {
  LOG(INFO) << "[Worker] RunDummy";
}

void Worker::RunMap(SArrayBinStream bin) {
  LOG(INFO) << "[Worker] [qid " << Qid() <<"] RunMap";
  int plan_id;
  bin >> plan_id;
  auto func = engine_elem_.function_store->GetMap(plan_id);
  PlanSpec plan = plan_map_[plan_id];
  engine_elem_.partition_tracker->SetPlan(plan); // set plan before run partition tracker
  engine_elem_.partition_tracker->RunAllMap([func, this](ShuffleMeta meta, std::shared_ptr<AbstractPartition> p,
                                            std::shared_ptr<AbstractMapProgressTracker> pt) {
    func(meta, p, engine_elem_.intermediate_store, pt);
  });
}

void Worker::LoadBlock(SArrayBinStream bin) {
  LOG(INFO) << "[Worker] LoadBlock";
  AssignedBlock block;
  bin >> block;
  loader_->Load(block);
}

void Worker::Distribute(SArrayBinStream bin) {
  LOG(INFO) << "[Worker] Distribute";
  int part_id;
  CollectionBuilderSpec builder;
  bin >> part_id >> builder;
  auto func = engine_elem_.function_store->GetCreatePartition(builder.collection_id);
  auto part = func(builder.data, part_id, builder.num_partition);
  engine_elem_.partition_manager->Insert(builder.collection_id, part_id, std::move(part));
  SArrayBinStream reply_bin;
  reply_bin << builder.collection_id << part_id << engine_elem_.node.id;
  SendMsgToScheduler(ScheduleFlag::kFinishDistribute, reply_bin);
}

void Worker::Exit() {
  exit_promise_.set_value();
}
void Worker::MapFinish() {
  LOG(INFO) << "[Worker] MapFinish";
}
void Worker::JoinFinish() {
  LOG(INFO) << "[Worker] JoinFinish";
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

}  // namespace xyz


