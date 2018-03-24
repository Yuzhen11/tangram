#include "core/worker/plan_controller.hpp"
#include "core/scheduler/control.hpp"
#include "core/partition/abstract_map_progress_tracker.hpp"
#include "glog/logging.h"

#include <limits>

namespace xyz {

struct FakeTracker : public AbstractMapProgressTracker {
  virtual void Report(int) override {}
};

PlanController::PlanController(Controller* controller)
  : controller_(controller) {
  fetch_executor_ = std::make_shared<Executor>(5);
}
 
void PlanController::Setup(SpecWrapper spec) {
  type_ = spec.type;
  CHECK(type_ == SpecWrapper::Type::kMapJoin
       || type_ == SpecWrapper::Type::kMapWithJoin);
  if (type_ == SpecWrapper::Type::kMapWithJoin) {
    auto* p = static_cast<MapWithJoinSpec*>(spec.spec.get());
    fetch_collection_id_ = p->with_collection_id;
  }
  auto* p = static_cast<MapJoinSpec*>(spec.spec.get());
  map_collection_id_ = p->map_collection_id;
  join_collection_id_ = p->join_collection_id;
  checkpoint_interval_ = p->checkpoint_interval;
  plan_id_ = spec.id;
  num_upstream_part_ = controller_->engine_elem_.collection_map->GetNumParts(map_collection_id_);
  num_join_part_ = controller_->engine_elem_.collection_map->GetNumParts(join_collection_id_);
  num_local_join_part_ = controller_->engine_elem_.partition_manager->GetNumLocalParts(join_collection_id_);
  num_local_map_part_ = controller_->engine_elem_.partition_manager->GetNumLocalParts(map_collection_id_);
  min_version_ = 0;
  staleness_ = p->staleness;
  expected_num_iter_ = p->num_iter;
  CHECK_NE(expected_num_iter_, 0);
  min_map_version_ = 0;
  min_join_version_ = 0;
  map_versions_.clear();
  join_versions_.clear();
  join_tracker_.clear();
  pending_joins_.clear();
  waiting_joins_.clear();

  auto parts = controller_->engine_elem_.partition_manager->Get(map_collection_id_);
  for (auto& part : parts) {
    map_versions_[part->id] = 0;
  }
  parts = controller_->engine_elem_.partition_manager->Get(join_collection_id_);
  for (auto& part : parts) {
    join_versions_[part->id] = 0;
  }

  SArrayBinStream reply_bin;
  ControllerMsg ctrl;
  ctrl.flag = ControllerMsg::Flag::kSetup;
  ctrl.node_id = controller_->engine_elem_.node.id;
  ctrl.plan_id = plan_id_;
  ctrl.version = -1;
  reply_bin << ctrl;
  SendMsgToScheduler(reply_bin);
}

void PlanController::StartPlan() {
  // if there is no join partition
  if (join_versions_.empty()) {
    min_join_version_ = expected_num_iter_;
    SendUpdateJoinVersionToScheduler();
  }
  TryRunSomeMaps();
}


void PlanController::UpdateVersion(SArrayBinStream bin) {
  int new_version;
  bin >> new_version;
  if (new_version == -1) { // finish plan
    SArrayBinStream bin;
    ControllerMsg ctrl;
    ctrl.flag = ControllerMsg::Flag::kFinish;
    ctrl.version = min_map_version_;
    ctrl.node_id = controller_->engine_elem_.node.id;
    ctrl.plan_id = plan_id_;
    bin << ctrl;
    SendMsgToScheduler(bin);
   
    DisplayTime();
    return;
  }
  CHECK_EQ(new_version, min_version_+1);
  min_version_ = new_version;
  TryRunSomeMaps();
}

void PlanController::FinishMap(SArrayBinStream bin) {
  int part_id;
  bin >> part_id;
  running_maps_.erase(part_id);
  if (map_versions_.find(part_id) == map_versions_.end()) {
    LOG(INFO) << "FinishMap for part not local";
    return;
  }
  int last_version = map_versions_[part_id];
  map_versions_[part_id] += 1;
  TryUpdateMapVersion();

  if (map_collection_id_ == join_collection_id_) {
    if (!pending_joins_[part_id][last_version].empty()) {
      while (!pending_joins_[part_id][last_version].empty()) {
        waiting_joins_[part_id].push_back(pending_joins_[part_id][last_version].front());
        pending_joins_[part_id][last_version].pop_front();
      }
      pending_joins_[part_id].erase(last_version);
    }
    // try run waiting joins if map_collection_id_ == join_collection_id_
    TryRunWaitingJoins(part_id);
  }
  // select other maps
  TryRunSomeMaps();
}

void PlanController::TryRunSomeMaps() {
  if (min_map_version_ == expected_num_iter_) {
    return;
  }
  // if there is no map partitions
  if (map_versions_.empty()) {
    min_map_version_ += 1;
    SendUpdateMapVersionToScheduler();
  }

  for (auto kv : map_versions_) {
    int part_id = kv.first;
    if (IsMapRunnable(part_id)) {
      int version = kv.second;
      // 0. get partition
      CHECK(controller_->engine_elem_.partition_manager->Has(map_collection_id_, part_id));
      auto p = controller_->engine_elem_.partition_manager->Get(map_collection_id_, part_id);
      RunMap(part_id, version, p);
    }
  }
}

bool PlanController::IsMapRunnable(int part_id) {
  // 1. check whether there is running map for this part
  if (running_maps_.find(part_id) != running_maps_.end()) {
    return false;
  }
  // 2. if mid == jid, check whether there is running join for this part
  if (map_collection_id_ == join_collection_id_
          && running_joins_.find(part_id) != running_joins_.end()) {
    return false;
  }
  // 3. check version
  int version = map_versions_[part_id];
  if (version == expected_num_iter_) {  // no need to run anymore
    return false;
  }
  if (version <= min_version_ + staleness_) {
    return true;
  } else {
    return false;
  }
}

bool PlanController::TryRunWaitingJoins(int part_id) {
  // if this part is migrating, do not join
  if (part_id == stop_joining_partition_) {
    return false;
  }

  if (running_joins_.find(part_id) != running_joins_.end()) {
    // someone is joining this part
    return false;
  }

  if (fetch_collection_id_ == join_collection_id_ 
          && running_fetches_[part_id] > 0) {
    // someone is fetching this part
    return false;
  }

  if (map_collection_id_ == join_collection_id_ 
          && running_maps_.find(part_id) != running_maps_.end()) {
    // someone is mapping this part
    return false;
  }

  // see whether there is any waiting joins
  auto& joins = waiting_joins_[part_id];
  if (!joins.empty()) {
    VersionedJoinMeta meta = joins.front();
    joins.pop_front();
    if (!meta.meta.is_fetch) RunJoin(meta);
    else RunFetchRequest(meta);
    return true;
  }
  return false;
}

void PlanController::FinishJoin(SArrayBinStream bin) {
  int part_id, upstream_part_id, version;
  bin >> part_id >> upstream_part_id >> version;
  // LOG(INFO) << "FinishJoin: partid, version: " << part_id << " " << version;
  running_joins_.erase(part_id);

  join_tracker_[part_id][version].insert(upstream_part_id);
  if (join_tracker_[part_id][version].size() == num_upstream_part_) {
    join_tracker_[part_id].erase(version);
    join_versions_[part_id] += 1;
    TryUpdateJoinVersion();

    bool runcp = TryCheckpoint(part_id);
    if (runcp) {
      return;
    }
  }
  bool run = TryRunWaitingJoins(part_id);
}

bool PlanController::TryCheckpoint(int part_id) {
  if (checkpoint_interval_ != 0 && join_versions_[part_id] % checkpoint_interval_ == 0) {
    int checkpoint_iter = join_versions_[part_id] / checkpoint_interval_;
    std::string dest_url = "/tmp/tmp/c"
        +std::to_string(join_collection_id_)
        +"-p"+std::to_string(part_id)
        +"-cp"+std::to_string(checkpoint_iter);

    CHECK(running_joins_.find(part_id) == running_joins_.end());
    running_joins_.insert({part_id, -1});
    // TODO: is it ok to use the fetch_executor? can it be block due to other operations?
    fetch_executor_->Add([this, part_id, dest_url]() {
      auto writer = controller_->io_wrapper_->GetWriter();
      CHECK(controller_->engine_elem_.partition_manager->Has(join_collection_id_, part_id));
      auto part = controller_->engine_elem_.partition_manager->Get(join_collection_id_, part_id);

      SArrayBinStream bin;
      part->ToBin(bin);
      bool rc = writer->Write(dest_url, bin.GetPtr(), bin.Size());
      CHECK_EQ(rc, 0);
      LOG(INFO) << "write checkpoint to " << dest_url;

      // Send finish checkpoint
      Message msg;
      msg.meta.sender = 0;
      msg.meta.recver = 0;
      msg.meta.flag = Flag::kOthers;
      SArrayBinStream ctrl_bin, plan_bin, reply_bin;
      ctrl_bin << ControllerFlag::kFinishCheckpoint;
      plan_bin << plan_id_; 
      reply_bin << part_id;

      msg.AddData(ctrl_bin.ToSArray());
      msg.AddData(plan_bin.ToSArray());
      msg.AddData(reply_bin.ToSArray());
      controller_->GetWorkQueue()->Push(msg);
    });
    return true;
  } else {
    return false;
  }
}

void PlanController::FinishCheckpoint(SArrayBinStream bin) {
  int part_id;
  bin >> part_id;
  LOG(INFO) << "finish checkpoint: " << part_id;
  CHECK(running_joins_.find(part_id) != running_joins_.end());
  running_joins_.erase(part_id);
  bool run = TryRunWaitingJoins(part_id);
}

int PlanController::GetSlowestMapPartitionId() {
  CHECK_EQ(map_versions_.size(), num_local_map_part_);
  int id = -1;
  for (auto kv: map_versions_) {
    if (kv.second == min_map_version_) {
      id = kv.first; 
    }
  }
  CHECK_NE(id, -1);
  return id;
}

void PlanController::TryUpdateMapVersion() {
  CHECK_EQ(map_versions_.size(), num_local_map_part_);
  for (auto kv: map_versions_) {
    if (kv.second == min_map_version_) {
      return;
    }
  }
  min_map_version_ += 1;
  // TODO send to scheduler
  SendUpdateMapVersionToScheduler();
}

void PlanController::TryUpdateJoinVersion() {
  CHECK_EQ(join_versions_.size(), num_local_join_part_);
  for (auto kv: join_versions_) {
    if (kv.second == min_join_version_) {
      return;
    }
  }
  min_join_version_ += 1;
  // TODO send to scheduler
  SendUpdateJoinVersionToScheduler();
}

void PlanController::SendUpdateMapVersionToScheduler() {
  LOG(INFO) << "[PlanController] update map version: " << min_map_version_
      << ", on " << controller_->engine_elem_.node.id 
      << ", for plan " << plan_id_;
  SArrayBinStream bin;
  ControllerMsg ctrl;
  ctrl.flag = ControllerMsg::Flag::kMap;
  ctrl.version = min_map_version_;
  ctrl.node_id = controller_->engine_elem_.node.id;
  ctrl.plan_id = plan_id_;
  bin << ctrl;
  SendMsgToScheduler(bin);
}
void PlanController::SendUpdateJoinVersionToScheduler() {
  LOG(INFO) << "[PlanController] update join version: " << min_join_version_
      << ", on " << controller_->engine_elem_.node.id
      << ", for plan " << plan_id_;
  SArrayBinStream bin;
  ControllerMsg ctrl;
  ctrl.flag = ControllerMsg::Flag::kJoin;
  ctrl.version = min_join_version_;
  ctrl.node_id = controller_->engine_elem_.node.id;
  ctrl.plan_id = plan_id_;
  bin << ctrl;
  SendMsgToScheduler(bin);
}

void PlanController::RunMap(int part_id, int version, 
        std::shared_ptr<AbstractPartition> p) {
  CHECK(running_maps_.find(part_id) == running_maps_.end());
  running_maps_.insert(part_id);

  controller_->engine_elem_.executor->Add([this, part_id, version, p]() {
    auto start = std::chrono::system_clock::now();
    auto pt = std::make_shared<FakeTracker>();
    // 1. map
    std::shared_ptr<AbstractMapOutput> map_output;
    if (type_ == SpecWrapper::Type::kMapJoin) {
      auto& map = controller_->engine_elem_.function_store->GetMap(plan_id_);
      map_output = map(p, pt); 
    } else if (type_ == SpecWrapper::Type::kMapWithJoin){
      auto& mapwith = controller_->engine_elem_.function_store->GetMapWith(plan_id_);
      map_output = mapwith(version, p, controller_->engine_elem_.fetcher, pt); 
    } else {
      CHECK(false);
    }
    // 2. serialize
    auto start2 = std::chrono::system_clock::now();

    int buffer_size = map_output->GetBufferSize();
    CHECK_EQ(buffer_size, num_join_part_);
    for (int i = 0; i < buffer_size; ++ i) {
      Message msg;
      msg.meta.sender = controller_->Qid();
      CHECK(controller_->engine_elem_.collection_map);
      msg.meta.recver = GetControllerActorQid(controller_->engine_elem_.collection_map->Lookup(join_collection_id_, i));
      msg.meta.flag = Flag::kOthers;
      SArrayBinStream ctrl_bin, plan_bin, ctrl2_bin;
      ctrl_bin << ControllerFlag::kReceiveJoin;

      plan_bin << plan_id_;

      VersionedShuffleMeta meta;
      meta.plan_id = plan_id_;
      meta.collection_id = join_collection_id_;
      meta.upstream_part_id = part_id;
      meta.part_id = i;
      meta.version = version;
      // now I reuse the local_mode variable used by fetch
      meta.local_mode = (msg.meta.recver == msg.meta.sender);  
      ctrl2_bin << meta;

      msg.AddData(ctrl_bin.ToSArray());
      msg.AddData(plan_bin.ToSArray());
      msg.AddData(ctrl2_bin.ToSArray());

      if (local_map_mode_ && msg.meta.recver == msg.meta.sender) {
        auto k = std::make_tuple(i, part_id, version);
        stream_store_.Insert(k, map_output->Get(i));
        SArrayBinStream dummy_bin;
        msg.AddData(dummy_bin.ToSArray());
        controller_->GetWorkQueue()->Push(msg);
      } else {
        auto bin = map_output->Get(i)->Serialize();
        msg.AddData(bin.ToSArray());
        controller_->engine_elem_.intermediate_store->Add(msg);
      }
    }
    Message msg;
    msg.meta.sender = 0;
    msg.meta.recver = 0;
    msg.meta.flag = Flag::kOthers;
    SArrayBinStream ctrl_bin, plan_bin, bin;
    ctrl_bin << ControllerFlag::kFinishMap;
    plan_bin << plan_id_;
    bin << part_id;
    msg.AddData(ctrl_bin.ToSArray());
    msg.AddData(plan_bin.ToSArray());
    msg.AddData(bin.ToSArray());
    controller_->GetWorkQueue()->Push(msg);
    
    auto end = std::chrono::system_clock::now();
    {
      std::unique_lock<std::mutex> lk(time_mu_);
      map_time_[part_id] = std::make_tuple(start, start2, end);
    }
  });
}

void PlanController::ReceiveJoin(Message msg) {
  CHECK_EQ(msg.data.size(), 4);
  SArrayBinStream ctrl2_bin, bin;
  ctrl2_bin.FromSArray(msg.data[2]);
  bin.FromSArray(msg.data[3]);
  VersionedShuffleMeta meta;
  ctrl2_bin >> meta;
  // LOG(INFO) << "ReceiveJoin: " << meta.DebugString();

  VersionedJoinMeta join_meta;
  join_meta.meta = meta;
  join_meta.bin = bin;

  // if already joined, omit it.
  // still need to check again in RunJoin as this upstream_part_id may be in waiting joins.
  if (IsJoinedBefore(meta)) {
    LOG(INFO) << "ignore join in ReceiveJoin, already joined: " << meta.DebugString();
    return;
  }

  if (map_collection_id_ == join_collection_id_) {
    if (meta.version >= map_versions_[meta.part_id]) {
      pending_joins_[meta.part_id][meta.version].push_back(join_meta);
      return;
    }
  }

  waiting_joins_[meta.part_id].push_back(join_meta);
  TryRunWaitingJoins(meta.part_id);
}

// check whether this upstream_part_id is joined already
bool PlanController::IsJoinedBefore(const VersionedShuffleMeta& meta) {
  if (join_tracker_[meta.part_id][meta.version].find(meta.upstream_part_id)
      != join_tracker_[meta.part_id][meta.version].end()) {
    LOG(INFO) << "ignore join, already joined: " << meta.DebugString();
    return true;
  } else {
    return false;
  }
}

void PlanController::RunJoin(VersionedJoinMeta meta) {
  if (IsJoinedBefore(meta.meta)) {
    return;
  }
  // LOG(INFO) << "RunJoin: " << meta.meta.DebugString();
  CHECK(running_joins_.find(meta.meta.part_id) == running_joins_.end());
  running_joins_.insert({meta.meta.part_id, meta.meta.upstream_part_id});
  // use the fetch_executor to avoid the case:
  // map wait for fetch, fetch wait for join, join is in running_joins_
  // but it cannot run because map does not finish and occupy the threadpool
  fetch_executor_->Add([this, meta]() {
    auto start = std::chrono::system_clock::now();

    if (local_map_mode_ && meta.meta.local_mode) {
      auto k = std::make_tuple(meta.meta.part_id, meta.meta.upstream_part_id, meta.meta.version);
      auto stream = stream_store_.Get(k);
      stream_store_.Remove(k);
      auto& join_func = controller_->engine_elem_.function_store->GetJoin2(plan_id_);
      CHECK(controller_->engine_elem_.partition_manager->Has(join_collection_id_, meta.meta.part_id));
      auto p = controller_->engine_elem_.partition_manager->Get(join_collection_id_, meta.meta.part_id);
      join_func(p, stream);
    } else {
      auto& join_func = controller_->engine_elem_.function_store->GetJoin(plan_id_);
      CHECK(controller_->engine_elem_.partition_manager->Has(join_collection_id_, meta.meta.part_id));
      auto p = controller_->engine_elem_.partition_manager->Get(join_collection_id_, meta.meta.part_id);
      join_func(p, meta.bin);
    }

    Message msg;
    msg.meta.sender = 0;
    msg.meta.recver = 0;
    msg.meta.flag = Flag::kOthers;
    SArrayBinStream ctrl_bin, plan_bin, bin;
    ctrl_bin << ControllerFlag::kFinishJoin;
    plan_bin << plan_id_; 
    bin << meta.meta.part_id << meta.meta.upstream_part_id << meta.meta.version;

    msg.AddData(ctrl_bin.ToSArray());
    msg.AddData(plan_bin.ToSArray());
    msg.AddData(bin.ToSArray());
    controller_->GetWorkQueue()->Push(msg);
    
    auto end = std::chrono::system_clock::now();
    {
      std::unique_lock<std::mutex> lk(time_mu_);
      join_time_[meta.meta.part_id][meta.meta.upstream_part_id] = std::make_pair(start, end);
    }
  });
}

void PlanController::SendMsgToScheduler(SArrayBinStream bin) {
  Message msg;
  msg.meta.sender = controller_->Qid();
  msg.meta.recver = 0;
  msg.meta.flag = Flag::kOthers;
  SArrayBinStream ctrl_bin;
  ctrl_bin << ScheduleFlag::kControl;
  msg.AddData(ctrl_bin.ToSArray());
  msg.AddData(bin.ToSArray());
  controller_->engine_elem_.sender->Send(std::move(msg));
}

void PlanController::ReceiveFetchRequest(Message msg) {
  CHECK_EQ(msg.data.size(), 3);
  SArrayBinStream ctrl2_bin, bin;
  ctrl2_bin.FromSArray(msg.data[1]);
  bin.FromSArray(msg.data[2]);
  FetchMeta received_fetch_meta;
  ctrl2_bin >> received_fetch_meta;
  CHECK(received_fetch_meta.plan_id == plan_id_);
  CHECK(controller_->engine_elem_.partition_manager->Has(received_fetch_meta.collection_id, received_fetch_meta.partition_id)) 
      << "cid: " << received_fetch_meta.collection_id << ", pid: " << received_fetch_meta.partition_id;

  VersionedJoinMeta fetch_meta;
  fetch_meta.meta.plan_id = plan_id_;
  fetch_meta.meta.collection_id = received_fetch_meta.collection_id;
  CHECK(fetch_meta.meta.collection_id == fetch_collection_id_);
  fetch_meta.meta.part_id = received_fetch_meta.partition_id;
  fetch_meta.meta.upstream_part_id = received_fetch_meta.upstream_part_id;
  fetch_meta.meta.version = received_fetch_meta.version;  // version -1 means fetch objs, others means fetch part
  fetch_meta.meta.is_fetch = true;
  fetch_meta.meta.local_mode = received_fetch_meta.local_mode;
  fetch_meta.bin = bin;
  fetch_meta.meta.sender = msg.meta.sender;
  CHECK_EQ(msg.meta.recver, controller_->Qid());
  fetch_meta.meta.recver = msg.meta.recver;
  
  CHECK(fetch_meta.meta.is_fetch == true);
  
  bool map_fetch = (fetch_meta.meta.collection_id == map_collection_id_);
  bool map_join = (map_collection_id_ == join_collection_id_);
  bool join_fetch = (fetch_meta.meta.collection_id == join_collection_id_);
  if (!map_fetch && !map_join && !join_fetch) { // all different collection
    RunFetchRequest(fetch_meta);
  }
  if (map_fetch && !map_join) { // map collection = fetch collection
    RunFetchRequest(fetch_meta);
  }
  if (map_join && !map_fetch) { // map collection = join collection
    RunFetchRequest(fetch_meta);
  }


  // join could be blocked by running_fetches
  if (join_fetch && !map_fetch) { // join collection = fetch collection
    if (running_joins_.find(fetch_meta.meta.part_id) != running_joins_.end()) {
      waiting_joins_[fetch_meta.meta.part_id].push_back(fetch_meta);
    } else {
      RunFetchRequest(fetch_meta);
    }
  }
  if (map_fetch && map_join) { // all the same collection
    if (running_joins_.find(fetch_meta.meta.part_id) != running_joins_.end()) {
      waiting_joins_[fetch_meta.meta.part_id].push_back(fetch_meta);
    } else { RunFetchRequest(fetch_meta); }
  }

}

void PlanController::RunFetchRequest(VersionedJoinMeta fetch_meta) {
  running_fetches_[fetch_meta.meta.part_id] += 1;

  // identify the version
  int version = -1;
  if (fetch_collection_id_ == join_collection_id_) {
    version = join_versions_[fetch_meta.meta.part_id];
  } else {
    version = std::numeric_limits<int>::max();
  }

  bool local_fetch = (GetNodeId(fetch_meta.meta.sender) == GetNodeId(fetch_meta.meta.recver));
  // LOG(INFO) << "run fetch: " << local_fetch << " " << fetch_meta.meta.version;
  if (fetch_meta.meta.local_mode && fetch_meta.meta.version != -1 && local_fetch) {  // for local fetch part
    // grant access to the fetcher
    // the fetcher should send kFinishFetch explicitly to release the fetch
    Message reply_msg;
    reply_msg.meta.sender = fetch_meta.meta.recver;
    reply_msg.meta.recver = fetch_meta.meta.sender;
    reply_msg.meta.flag = Flag::kOthers;
    SArrayBinStream ctrl_reply_bin, ctrl2_reply_bin;
    ctrl_reply_bin << FetcherFlag::kFetchPartReplyLocal;
    FetchMeta meta;
    meta.plan_id = plan_id_;
    meta.upstream_part_id = fetch_meta.meta.upstream_part_id;
    meta.collection_id = fetch_meta.meta.collection_id;
    meta.partition_id = fetch_meta.meta.part_id;
    meta.version = version;
    ctrl2_reply_bin << meta;
    reply_msg.AddData(ctrl_reply_bin.ToSArray());
    reply_msg.AddData(ctrl2_reply_bin.ToSArray());
    controller_->engine_elem_.sender->Send(std::move(reply_msg));
    // fetcher should release the fetch
  } else {
    fetch_executor_->Add([this, fetch_meta, version] {
      Fetch(fetch_meta, version);
    });
  }

}

void PlanController::Fetch(VersionedJoinMeta fetch_meta, int version) {
  CHECK(controller_->engine_elem_.partition_manager->Has(fetch_meta.meta.collection_id, fetch_meta.meta.part_id)) << fetch_meta.meta.collection_id << " " <<  fetch_meta.meta.part_id;
  auto part = controller_->engine_elem_.partition_manager->Get(fetch_meta.meta.collection_id, fetch_meta.meta.part_id);
  SArrayBinStream reply_bin;
  if (fetch_meta.meta.version == -1) {  // fetch objs
    auto& func = controller_->engine_elem_.function_store->GetGetter(fetch_meta.meta.collection_id);
    reply_bin = func(fetch_meta.bin, part);
  } else {  // fetch part
    part->ToBin(reply_bin);
  }
  // reply, send to fetcher
  Message reply_msg;
  reply_msg.meta.sender = fetch_meta.meta.recver;
  reply_msg.meta.recver = fetch_meta.meta.sender;
  reply_msg.meta.flag = Flag::kOthers;
  SArrayBinStream ctrl_reply_bin, ctrl2_reply_bin;
  if (fetch_meta.meta.version == -1) {  // fetch objs
    ctrl_reply_bin << FetcherFlag::kFetchObjsReply;
  } else {  // fetch part
    ctrl_reply_bin << FetcherFlag::kFetchPartReplyRemote;
  }
  FetchMeta meta;
  meta.plan_id = plan_id_;
  meta.upstream_part_id = fetch_meta.meta.upstream_part_id;
  meta.collection_id = fetch_meta.meta.collection_id;
  meta.partition_id = fetch_meta.meta.part_id;
  meta.version = version;
  ctrl2_reply_bin << meta;
  reply_msg.AddData(ctrl_reply_bin.ToSArray());
  reply_msg.AddData(ctrl2_reply_bin.ToSArray());
  reply_msg.AddData(reply_bin.ToSArray());
  controller_->engine_elem_.sender->Send(std::move(reply_msg));
  
  // send to controller
  Message msg;
  msg.meta.sender = 0;
  msg.meta.recver = 0;
  msg.meta.flag = Flag::kOthers;
  SArrayBinStream ctrl_bin, plan_bin, bin;
  ctrl_bin << ControllerFlag::kFinishFetch;
  plan_bin << plan_id_;
  bin << fetch_meta.meta.part_id << fetch_meta.meta.upstream_part_id;
  msg.AddData(ctrl_bin.ToSArray());
  msg.AddData(plan_bin.ToSArray());
  msg.AddData(bin.ToSArray());
  controller_->GetWorkQueue()->Push(msg);
}

void PlanController::FinishFetch(SArrayBinStream bin) {
  int part_id, upstream_part_id;
  bin >> part_id >> upstream_part_id;
  // LOG(INFO) << "FinishFetch: " << part_id << " " << upstream_part_id;
  running_fetches_[part_id] -= 1;
  bool run = TryRunWaitingJoins(part_id);
}

void PlanController::MigratePartition(Message msg) {
  CHECK_EQ(msg.data.size(), 4);
  SArrayBinStream ctrl2_bin, bin;
  ctrl2_bin.FromSArray(msg.data[2]);
  bin.FromSArray(msg.data[3]);
  MigrateMeta2 migrate_meta;
  ctrl2_bin >> migrate_meta;
  if (migrate_meta.flag == MigrateMeta2::MigrateFlag::kStartMigrate) {
    MigratePartitionStartMigrate(migrate_meta);
  } else if (migrate_meta.flag == MigrateMeta2::MigrateFlag::kFlushAll) {
    MigratePartitionReceiveFlushAll(migrate_meta);
  } else if (migrate_meta.flag == MigrateMeta2::MigrateFlag::kDest){
    MigratePartitionDest(migrate_meta);
  } else {
    CHECK(false);
  }
}

void PlanController::MigratePartitionStartMigrate(MigrateMeta2 migrate_meta) {
  // TODO: update collection_map
  if (migrate_meta.from_id != controller_->engine_elem_.node.id) {
    Message flush_msg;
    flush_msg.meta.sender = controller_->engine_elem_.node.id;
    flush_msg.meta.recver = GetControllerActorQid(migrate_meta.from_id);
    flush_msg.meta.flag = Flag::kOthers;
    SArrayBinStream ctrl_bin, plan_bin, ctrl2_bin;
    ctrl_bin << ControllerFlag::kMigratePartition;
    plan_bin << plan_id_;
    ctrl2_bin << migrate_meta;
    flush_msg.AddData(ctrl_bin.ToSArray());
    flush_msg.AddData(plan_bin.ToSArray());
    flush_msg.AddData(ctrl2_bin.ToSArray());
    controller_->engine_elem_.sender->Send(std::move(flush_msg));
    LOG(INFO) << "flush all: " << migrate_meta.DebugString();
  } else {
    // stop the update to the partition.
    if (migrate_meta.collection_id == join_collection_id_
        && join_versions_.find(migrate_meta.partition_id) != join_versions_.end()) {
      // is join collection and local
      stop_joining_partition_ = migrate_meta.partition_id;
    }
  }
}

void PlanController::MigratePartitionReceiveFlushAll(MigrateMeta2 migrate_meta) {
  CHECK_EQ(migrate_meta.from_id, controller_->engine_elem_.node.id) << "only w_a receives FlushAll";
  flush_all_count_ += 1;
  // if (flush_all_count_ == ) {
  //   // flush the buffered data structure to to_id
  // }
}

void PlanController::MigratePartitionDest(MigrateMeta2 migrate_meta) {
  CHECK_EQ(migrate_meta.to_id, controller_->engine_elem_.node.id) << "only w_b receive this";
}

void PlanController::RequestPartition(SArrayBinStream bin) {
  MigrateMeta migrate_meta;
  bin >> migrate_meta;
  CHECK_EQ(migrate_meta.collection_id, map_collection_id_) << "only consider map_collection first";
  CHECK_NE(migrate_meta.collection_id, join_collection_id_) << "only consider map_collection first";
  migrate_meta.partition_id = GetSlowestMapPartitionId();
  migrate_map_parts_.insert(migrate_meta.partition_id);
  CHECK(map_versions_.find(migrate_meta.partition_id) != map_versions_.end());

  CHECK(controller_->engine_elem_.partition_manager->Has(
    migrate_meta.collection_id, migrate_meta.partition_id)) << migrate_meta.collection_id << " " <<  migrate_meta.partition_id;
  auto part = controller_->engine_elem_.partition_manager->Get(
    migrate_meta.collection_id, migrate_meta.partition_id);
  SArrayBinStream reply_bin;
  part->ToBin(reply_bin);  // serialize

  migrate_meta.current_map_version = map_versions_[migrate_meta.partition_id];

  // TODO: clear related information in this controller

  Message msg;
  msg.meta.sender = controller_->engine_elem_.node.id;
  msg.meta.recver = GetControllerActorQid(migrate_meta.to_id);
  msg.meta.flag = Flag::kOthers;
  SArrayBinStream ctrl_bin, plan_bin, ctrl2_bin;
  ctrl_bin << ControllerFlag::kReceivePartition;
  plan_bin << plan_id_;
  ctrl2_bin << migrate_meta;
  msg.AddData(ctrl_bin.ToSArray());
  msg.AddData(plan_bin.ToSArray());
  msg.AddData(ctrl2_bin.ToSArray());
  msg.AddData(reply_bin.ToSArray());
  controller_->engine_elem_.sender->Send(std::move(msg));
  LOG(INFO) << "migrating: " << migrate_meta.DebugString();
}

// TODO: only support MR style speculative execution.
void PlanController::ReceivePartition(Message msg) {
  CHECK_EQ(msg.data.size(), 4);
  SArrayBinStream ctrl2_bin, bin;
  ctrl2_bin.FromSArray(msg.data[2]);
  bin.FromSArray(msg.data[3]);
  MigrateMeta migrate_meta;
  ctrl2_bin >> migrate_meta;

  // TODO: update some control data structure?

  auto& func = controller_->engine_elem_.function_store
      ->GetCreatePart(migrate_meta.collection_id);
  auto p = func();
  p->FromBin(bin);  // now I serialize in the controller thread

  RunMap(migrate_meta.partition_id, migrate_meta.current_map_version, p);
}

void PlanController::DisplayTime() {
  double avg_map_time = 0;
  double avg_map_stime = 0;
  double avg_join_time = 0;
  std::chrono::duration<double> duration;
  for (auto& x : map_time_) {
    duration = std::get<1>(x.second) - std::get<0>(x.second);
    avg_map_time += duration.count();
    duration = std::get<2>(x.second) - std::get<1>(x.second);
    avg_map_stime += duration.count();
  }
  for (auto& x : join_time_) {
    for (auto& y : x.second) {
      duration = y.second.second - y.second.first;
      avg_join_time += duration.count();
    }
  }
  avg_map_time /= num_local_map_part_;
  avg_map_stime /= num_local_map_part_;
  avg_join_time = avg_join_time / num_local_join_part_;// num_upstream_part_;
  // nan if num is zero
  LOG(INFO) << "avg map time: " << avg_map_time << " ,avg map serialization time: " << avg_map_stime << " ,avg join time: " << avg_join_time;
} 

}  // namespace

