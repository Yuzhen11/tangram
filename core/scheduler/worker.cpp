#include "core/scheduler/worker.hpp"
#include "core/plan/collection_spec.hpp"
#include "core/queue_node_map.hpp"
#include "core/shuffle_meta.hpp"

#include "core/plan/spec_wrapper.hpp"

namespace xyz {

void Worker::Wait() {
  std::future<void> f = exit_promise_.get_future();
  f.get();
}

void Worker::RegisterProgram() {
  LOG(INFO) << WorkerId() << "RegisterProgram";
  CHECK(is_program_set_);
  ready_ = true;

  SArrayBinStream bin;
  WorkerInfo info;
  info.num_local_threads = engine_elem_.num_local_threads;
  bin << info;
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
  case ScheduleFlag::kWritePartition: {
    WritePartition(bin);
    break;
  }
  default:
    CHECK(false);
  }
}

void Worker::InitWorkers(SArrayBinStream bin) {
  std::unordered_map<int, CollectionView> collection_map;
  bin >> collection_map;
  engine_elem_.collection_map->Init(collection_map);
  SArrayBinStream dummy_bin;
  SendMsgToScheduler(ScheduleFlag::kInitWorkersReply, dummy_bin);
}

void Worker::RunDummy() { LOG(INFO) << WorkerId() << "RunDummy"; }

void Worker::RunMap(SArrayBinStream bin) {
  // PlanSpec plan;
  SpecWrapper spec;
  bin >> spec;
  auto* p = static_cast<MapJoinSpec*>(spec.spec.get());
  PlanSpec plan;
  plan.plan_id = spec.id;
  plan.map_collection_id = p->map_collection_id;
  plan.join_collection_id = p->join_collection_id;
  LOG(INFO) << WorkerId() << "RunMap: " << plan.DebugString();
  auto func = engine_elem_.function_store->GetMap(plan.plan_id);
  engine_elem_.partition_tracker->SetPlan(plan); // set plan before run partition tracker
  engine_elem_.partition_tracker->RunAllMap(
      [func, this](ShuffleMeta meta, std::shared_ptr<AbstractPartition> p,
                   std::shared_ptr<AbstractMapProgressTracker> pt) {
        func(meta, p, engine_elem_.intermediate_store, pt);
  });
}

void Worker::LoadBlock(SArrayBinStream bin) {
  AssignedBlock block;
  bin >> block;
  LOG(INFO) << WorkerId() << "LoadBlock: " << block.DebugString();
  block_reader_wrapper_->ReadBlock(block, 
  engine_elem_.function_store->GetCreatePartFromBlockReader(block.collection_id),
  [this](SArrayBinStream bin) {
    SendMsgToScheduler(ScheduleFlag::kFinishBlock, bin);
  }
  );
}

void Worker::Distribute(SArrayBinStream bin) {
  // LOG(INFO) << WorkerId() << "[Worker] Distribute";
  int part_id;
  DistributeSpec spec;
  bin >> part_id;
  spec.FromBin(bin);
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
    [](std::shared_ptr<AbstractPartition> p, std::shared_ptr<AbstractWriter> writer, std::string url) { 
      SArrayBinStream bin;
      p->ToBin(bin);
      bool rc = writer->Write(url, bin.GetPtr(), bin.Size());
      CHECK_EQ(rc, 0);
    }, 
    [this](SArrayBinStream bin) {
      SendMsgToScheduler(ScheduleFlag::kFinishCheckPoint, bin);
    }
  );
}

void Worker::WritePartition(SArrayBinStream bin) {
  int collection_id, part_id;
  std::string dest_url;
  bin >> collection_id >> part_id >> dest_url;

  writer_->Write(collection_id, part_id, dest_url, 
    engine_elem_.function_store->GetWritePartFunc(collection_id),  // get the write_part_func from function_store
    [this](SArrayBinStream bin) {
      SendMsgToScheduler(ScheduleFlag::kFinishWritePartition, bin);
    }
  );
}

void Worker::Exit() { 
  LOG(INFO) << WorkerId() << "Exit";
  exit_promise_.set_value(); 
}
void Worker::MapFinish() {
  LOG(INFO) << WorkerId() << "MapFinish";
}
void Worker::JoinFinish() {
  LOG(INFO) << WorkerId() << "JoinFinish";
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
