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
  local_map_mode_ = true;
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
  checkpoint_path_ = p->checkpoint_path;
  if (checkpoint_interval_ != 0) {
    CHECK(checkpoint_path_.size());
  }
  plan_id_ = spec.id;
  num_upstream_part_ = controller_->engine_elem_.collection_map->GetNumParts(map_collection_id_);
  num_join_part_ = controller_->engine_elem_.collection_map->GetNumParts(join_collection_id_);
  num_local_join_part_ = controller_->engine_elem_.partition_manager->GetNumLocalParts(join_collection_id_);
  num_local_map_part_ = controller_->engine_elem_.partition_manager->GetNumLocalParts(map_collection_id_);
  min_version_ = 0;
  staleness_ = p->staleness;
  expected_num_iter_ = p->num_iter;
  CHECK_NE(expected_num_iter_, 0);
  map_versions_.clear();
  join_versions_.clear();
  join_tracker_.clear();
  pending_joins_.clear();
  waiting_joins_.clear();
  int combine_timeout = p->combine_timeout;
  delayed_combiner_ = std::make_shared<DelayedCombiner>(this, combine_timeout);

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
  TryRunSomeMaps();
}


void PlanController::UpdateVersion(SArrayBinStream bin) {
  int new_version;
  bin >> new_version;
  if (new_version == -1) { // finish plan
    SArrayBinStream bin;
    ControllerMsg ctrl;
    ctrl.flag = ControllerMsg::Flag::kFinish;
    ctrl.version = -1;
    ctrl.node_id = controller_->engine_elem_.node.id;
    ctrl.plan_id = plan_id_;
    bin << ctrl;
    SendMsgToScheduler(bin);
   
    DisplayTime();
    return;
  }
  CHECK_EQ(new_version, min_version_+1);
  min_version_ = new_version;
  for (auto& part_version : join_tracker_) {
    part_version.second.erase(new_version-1);  // erase old join_tracker content
  }
  TryRunSomeMaps();
}

void PlanController::FinishMap(SArrayBinStream bin) {
  int part_id;
  bool update_version;
  bin >> part_id >> update_version;
  running_maps_.erase(part_id);
  if (!update_version) {
    return;
  }

  if (map_versions_.find(part_id) == map_versions_.end()) {
    LOG(INFO) << "FinishMap for part not local";
    return;
  }
  int last_version = map_versions_[part_id];
  map_versions_[part_id] += 1;
  ReportFinishPart(ControllerMsg::Flag::kMap, part_id, map_versions_[part_id]);

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
  if (min_version_ == expected_num_iter_) {
    return;
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
  // 3. if mid == jid, check whether this part is migrating
  {
    std::lock_guard<std::mutex> lk(stop_joining_partitions_mu_);
    if (map_collection_id_ == join_collection_id_
            && stop_joining_partitions_.find(part_id) != stop_joining_partitions_.end()) {
      return false;
    }
  }
  // 4. check version
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
  // if some fetches are in the waiting_joins_, 
  // join_collection_id_ == fetch_collection_id_
  {
    std::lock_guard<std::mutex> lk(stop_joining_partitions_mu_);
    if (stop_joining_partitions_.find(part_id) != stop_joining_partitions_.end()) {
      // if this part is migrating, do not join or fetch
      return false;
    }
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
    if (!meta.meta.is_fetch) {
      RunJoin(meta);
    } else {
      RunFetchRequest(meta);
    }
    return true;
  }
  return false;
}

void PlanController::FinishJoin(SArrayBinStream bin) {
  int part_id, version;
  std::vector<int> upstream_part_ids;
  bin >> part_id >> version >> upstream_part_ids;
  // LOG(INFO) << "FinishJoin: partid, version: " << part_id << " " << version;
  running_joins_.erase(part_id);

  for (auto upstream_part_id : upstream_part_ids) {
    join_tracker_[part_id][version].insert(upstream_part_id);
  }
  if (join_tracker_[part_id][version].size() == num_upstream_part_) {
    // join_tracker_[part_id].erase(version);  // TODO: should not remove
    join_versions_[part_id] += 1;
    ReportFinishPart(ControllerMsg::Flag::kJoin, part_id, join_versions_[part_id]);

    bool runcp = TryCheckpoint(part_id);
    if (runcp) {
      return;
    }
  }
  TryRunWaitingJoins(part_id);
}

bool PlanController::TryCheckpoint(int part_id) {
  if (checkpoint_interval_ != 0 && join_versions_[part_id] % checkpoint_interval_ == 0) {
    int checkpoint_iter = join_versions_[part_id] / checkpoint_interval_;
    std::string dest_url = checkpoint_path_ + 
        "/cp-" + std::to_string(checkpoint_iter);
    dest_url = GetCheckpointUrl(dest_url, join_collection_id_, part_id);

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
  TryRunWaitingJoins(part_id);
  TryRunSomeMaps();
}

void PlanController::ReportFinishPart(ControllerMsg::Flag flag, 
        int part_id, int version) {
  SArrayBinStream bin;
  ControllerMsg ctrl;
  ctrl.flag = flag;
  ctrl.version = version;
  ctrl.node_id = controller_->engine_elem_.node.id;
  ctrl.plan_id = plan_id_;
  ctrl.part_id = part_id;
  bin << ctrl;
  SendMsgToScheduler(bin);
}

void PlanController::RunMap(int part_id, int version, 
        std::shared_ptr<AbstractPartition> p) {
  CHECK(running_maps_.find(part_id) == running_maps_.end());
  running_maps_.insert(part_id);
  
  std::function<void(int, int, Controller*)> AbortMap = [](int plan_id_, int part_id, Controller* controller_){
    //map a partition to be migrated
    //avoid a part of ignore messages
    //need to send a finish map message
    LOG(INFO) << "[RunMap] map task on a migrating partition: " << part_id <<" submitted, stop map and send finish map msg";
    Message msg;
    msg.meta.sender = 0;
    msg.meta.recver = 0;
    msg.meta.flag = Flag::kOthers;
    SArrayBinStream ctrl_bin, plan_bin, bin;
    ctrl_bin << ControllerFlag::kFinishMap;
    bool update_version = false;
    plan_bin << plan_id_;
    bin << part_id << update_version;
    msg.AddData(ctrl_bin.ToSArray());
    msg.AddData(plan_bin.ToSArray());
    msg.AddData(bin.ToSArray());
    controller_->GetWorkQueue()->Push(msg);
  };

  controller_->engine_elem_.executor->Add([this, part_id, version, p, AbortMap]() {
    auto start = std::chrono::system_clock::now();
    auto pt = std::make_shared<FakeTracker>();

    {
      std::lock_guard<std::mutex> lk(stop_joining_partitions_mu_);
      if (stop_joining_partitions_.find(part_id) != stop_joining_partitions_.end()) {
        AbortMap(plan_id_, part_id, controller_);
        return;
      }
    }
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
  
    {
      std::lock_guard<std::mutex> lk(stop_joining_partitions_mu_);
      if (stop_joining_partitions_.find(part_id) != stop_joining_partitions_.end()) {
        AbortMap(plan_id_, part_id, controller_);
        return;
      }
    }
    // 2. send to delayed_combiner
    auto start2 = std::chrono::system_clock::now();
    CHECK(delayed_combiner_);
    delayed_combiner_->AddMapOutput(part_id, version, map_output);
    Message msg;
    msg.meta.sender = 0;
    msg.meta.recver = 0;
    msg.meta.flag = Flag::kOthers;
    SArrayBinStream ctrl_bin, plan_bin, bin;
    ctrl_bin << ControllerFlag::kFinishMap;
    bool update_version = true;
    plan_bin << plan_id_;
    bin << part_id << update_version;
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

  if (join_versions_.find(meta.part_id) == join_versions_.end()) {
    // if receive something that is not belong to here
    buffered_requests_[meta.part_id].push_back(join_meta);
    LOG(INFO) << "part to migrate: " << meta.part_id <<  ", buffered requsts size: " << buffered_requests_[meta.part_id].size();
    return;
  }

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
  // LOG(INFO) << meta.meta.DebugString();
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
      std::tuple<int, std::vector<int>, int> k;
      if (meta.meta.ext_upstream_part_ids.empty()) {
        k = std::make_tuple(meta.meta.part_id, std::vector<int>{meta.meta.upstream_part_id}, meta.meta.version);
      } else {
        k = std::make_tuple(meta.meta.part_id, meta.meta.ext_upstream_part_ids, meta.meta.version);
      }
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
    bin << meta.meta.part_id;
    bin << meta.meta.version; 
    if (meta.meta.ext_upstream_part_ids.empty()) {
      bin << std::vector<int>{meta.meta.upstream_part_id};  // still need to make it vector
    } else {
      bin << meta.meta.ext_upstream_part_ids;
    }

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

  bool join_fetch = (fetch_meta.meta.collection_id == join_collection_id_);

  if (!join_fetch) {
    RunFetchRequest(fetch_meta);
    return;
  }

  // otherise join_fetch
  CHECK(join_fetch);

  if (join_versions_.find(fetch_meta.meta.part_id) == join_versions_.end()) {
    // if receive something that is not belong to here
    buffered_requests_[fetch_meta.meta.part_id].push_back(fetch_meta);
    return;
  }

  {
    std::lock_guard<std::mutex> lk(stop_joining_partitions_mu_);
    if (stop_joining_partitions_.find(fetch_meta.meta.part_id) != stop_joining_partitions_.end()) {
      // if migrating this part
      waiting_joins_[fetch_meta.meta.part_id].push_back(fetch_meta);
      return;
    }
  }

  if (running_joins_.find(fetch_meta.meta.part_id) != running_joins_.end()) {
    // if this part is joining
    waiting_joins_[fetch_meta.meta.part_id].push_back(fetch_meta);
  } else {
    RunFetchRequest(fetch_meta);
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
  TryRunWaitingJoins(part_id);
}

void PlanController::MigratePartition(Message msg) {
  CHECK_GE(msg.data.size(), 3);
  SArrayBinStream ctrl2_bin, bin;
  ctrl2_bin.FromSArray(msg.data[2]);
  MigrateMeta migrate_meta;
  ctrl2_bin >> migrate_meta;
  if (migrate_meta.flag == MigrateMeta::MigrateFlag::kStartMigrate) {
    // update collection_map
    CollectionView collection_view;
    ctrl2_bin >> collection_view;
    {
      std::lock_guard<std::mutex> lk(migrate_mu_);
      controller_->engine_elem_.collection_map->Insert(collection_view);
      // send msg to from_id
      MigratePartitionStartMigrate(migrate_meta);
    }
  } else if (migrate_meta.flag == MigrateMeta::MigrateFlag::kFlushAll) {
    MigratePartitionReceiveFlushAll(migrate_meta);
  } else if (migrate_meta.flag == MigrateMeta::MigrateFlag::kDest){
    MigratePartitionDest(msg);
  } else if (migrate_meta.flag == MigrateMeta::MigrateFlag::kStartMigrateMapOnly){
    MigratePartitionStartMigrateMapOnly(migrate_meta);
  } else if (migrate_meta.flag == MigrateMeta::MigrateFlag::kReceiveMapOnly){
    MigratePartitionReceiveMapOnly(msg);
  } else {
    CHECK(false);
  }
}

void PlanController::MigratePartitionStartMigrate(MigrateMeta migrate_meta) {
  migrate_meta.flag = MigrateMeta::MigrateFlag::kFlushAll;
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
  LOG(INFO) << "[Migrate] Send Flush signal on node: " << controller_->engine_elem_.node.id 
      << ": " << migrate_meta.DebugString();

  if (migrate_meta.from_id == controller_->engine_elem_.node.id) {
    // stop the update to the partition.
    CHECK_EQ(migrate_meta.collection_id, join_collection_id_) << "the migrate collection must be the join collection";
    // TODO: now the migrate partition must be join_collection
    CHECK(join_versions_.find(migrate_meta.partition_id) != join_versions_.end());
    {
      std::lock_guard<std::mutex> lk(stop_joining_partitions_mu_);
      CHECK(stop_joining_partitions_.find(migrate_meta.partition_id) == stop_joining_partitions_.end());
      stop_joining_partitions_.insert(migrate_meta.partition_id);
    }
  }
}

void PlanController::MigratePartitionReceiveFlushAll(MigrateMeta migrate_meta) {
  CHECK_EQ(migrate_meta.from_id, controller_->engine_elem_.node.id) << "only w_a receives FlushAll";
  if (flush_all_count_.find(migrate_meta.partition_id) == flush_all_count_.end()){
    flush_all_count_[migrate_meta.partition_id] = 0;
  };
  flush_all_count_[migrate_meta.partition_id] += 1;
  LOG(INFO) << "[Migrate] Received one Flush signal";
  if (flush_all_count_[migrate_meta.partition_id] == migrate_meta.num_nodes) {
    if (running_joins_.find(migrate_meta.partition_id) != running_joins_.end()) {
      // if there is a join/fetch task for this part
      // push a msg to the msg queue to run this function again
      LOG(INFO) << "there is a join/fetch for part, try to migrate partition and msgs later " << migrate_meta.partition_id;
      flush_all_count_[migrate_meta.partition_id] -= 1;
      CHECK(migrate_meta.flag == MigrateMeta::MigrateFlag::kFlushAll);
      Message flush_msg;
      flush_msg.meta.sender = controller_->engine_elem_.node.id;
      flush_msg.meta.recver = controller_->engine_elem_.node.id;
      flush_msg.meta.flag = Flag::kOthers;
      SArrayBinStream ctrl_bin, plan_bin, ctrl2_bin;
      ctrl_bin << ControllerFlag::kMigratePartition;
      plan_bin << plan_id_;
      ctrl2_bin << migrate_meta;
      flush_msg.AddData(ctrl_bin.ToSArray());
      flush_msg.AddData(plan_bin.ToSArray());
      flush_msg.AddData(ctrl2_bin.ToSArray());
      controller_->GetWorkQueue()->Push(flush_msg);
      return;
    }
    flush_all_count_[migrate_meta.partition_id] = 0;

    // flush the buffered data structure to to_id
    {
      std::lock_guard<std::mutex> lk(stop_joining_partitions_mu_);
      CHECK(stop_joining_partitions_.find(migrate_meta.partition_id) != 
          stop_joining_partitions_.end());
      stop_joining_partitions_.erase(migrate_meta.partition_id);
    }
    LOG(INFO) << "[Migrate] Received all Flush signal for partition "
      << migrate_meta.partition_id <<", send everything to dest";
    // now I don't care about the complexity
    // set the flag
    migrate_meta.flag = MigrateMeta::MigrateFlag::kDest;
    // construct the MigrateData
    MigrateData data;
    if (map_collection_id_ == join_collection_id_) {
      data.map_version = map_versions_[migrate_meta.partition_id];
      map_versions_.erase(migrate_meta.partition_id);
      num_local_map_part_ -= 1;
      // TryUpdateMapVersion();
    }
    data.join_version = join_versions_[migrate_meta.partition_id];
    data.pending_joins = std::move(pending_joins_[migrate_meta.partition_id]);
    data.waiting_joins = std::move(waiting_joins_[migrate_meta.partition_id]);

    auto serialize_from_stream_store = [this](VersionedJoinMeta& meta) {
      std::tuple<int, std::vector<int>, int> k;
      if (meta.meta.ext_upstream_part_ids.empty()) {
        k = std::make_tuple(meta.meta.part_id, std::vector<int>{meta.meta.upstream_part_id}, meta.meta.version);
      } else {
        k = std::make_tuple(meta.meta.part_id, meta.meta.ext_upstream_part_ids, meta.meta.version);
      }
      auto stream = stream_store_.Get(k);
      stream_store_.Remove(k);
      auto bin = stream->Serialize();
      meta.bin = bin;
      meta.meta.local_mode = false;
    };
    for (auto& version_joins : data.pending_joins) {
      for (auto& join_meta : version_joins.second) {
        if (local_map_mode_ && join_meta.meta.local_mode) {
          serialize_from_stream_store(join_meta);
        }
      } 
    }
    for (auto& join_meta : data.waiting_joins) {
      if (local_map_mode_ && join_meta.meta.local_mode) {
        serialize_from_stream_store(join_meta);
      }
    } 
    data.join_tracker = std::move(join_tracker_[migrate_meta.partition_id]);
    join_versions_.erase(migrate_meta.partition_id);
    num_local_join_part_ -= 1;
    pending_joins_.erase(migrate_meta.partition_id);
    waiting_joins_.erase(migrate_meta.partition_id);
    join_tracker_.erase(migrate_meta.partition_id);
    Message msg;
    msg.meta.sender = controller_->engine_elem_.node.id;
    msg.meta.recver = GetControllerActorQid(migrate_meta.to_id);
    msg.meta.flag = Flag::kOthers;
    SArrayBinStream ctrl_bin, plan_bin, ctrl2_bin, bin1, bin2;
    ctrl_bin << ControllerFlag::kMigratePartition;
    plan_bin << plan_id_;
    ctrl2_bin << migrate_meta;
    bin1 << data;

    // construct the partition bin
    CHECK(controller_->engine_elem_.partition_manager->Has(
      migrate_meta.collection_id, migrate_meta.partition_id)) << migrate_meta.collection_id << " " <<  migrate_meta.partition_id;
    auto part = controller_->engine_elem_.partition_manager->Get(
      migrate_meta.collection_id, migrate_meta.partition_id);
    part->ToBin(bin2);  // serialize
    controller_->engine_elem_.partition_manager->Remove(migrate_meta.collection_id, migrate_meta.partition_id);

    msg.AddData(ctrl_bin.ToSArray());
    msg.AddData(plan_bin.ToSArray());
    msg.AddData(ctrl2_bin.ToSArray());
    msg.AddData(bin1.ToSArray());
    msg.AddData(bin2.ToSArray());
    controller_->engine_elem_.sender->Send(std::move(msg));
  }
}

void PlanController::MigratePartitionDest(Message msg) {
  CHECK_EQ(msg.data.size(), 5);
  SArrayBinStream ctrl2_bin, bin1, bin2;
  ctrl2_bin.FromSArray(msg.data[2]);
  bin1.FromSArray(msg.data[3]);
  bin2.FromSArray(msg.data[4]);
  MigrateMeta migrate_meta;
  MigrateData migrate_data;
  ctrl2_bin >> migrate_meta;
  bin1 >> migrate_data;
  CHECK_EQ(migrate_meta.to_id, controller_->engine_elem_.node.id) << "only w_b receive this";

  LOG(INFO) << "[MigrateDone] " << migrate_data.DebugString();
  if (map_collection_id_ == join_collection_id_) {
    CHECK(map_versions_.find(migrate_meta.partition_id) == map_versions_.end());
    map_versions_[migrate_meta.partition_id] = migrate_data.map_version;
    num_local_map_part_ += 1;
    // TryUpdateMapVersion();
  } else {
    migrate_data.map_version = -1;
  }
  CHECK(join_versions_.find(migrate_meta.partition_id) == join_versions_.end());
  CHECK(pending_joins_.find(migrate_meta.partition_id) == pending_joins_.end());
  CHECK(waiting_joins_.find(migrate_meta.partition_id) == waiting_joins_.end());
  CHECK(join_tracker_.find(migrate_meta.partition_id) == join_tracker_.end());
  join_versions_[migrate_meta.partition_id] = migrate_data.join_version;
  pending_joins_[migrate_meta.partition_id] = std::move(migrate_data.pending_joins);
  waiting_joins_[migrate_meta.partition_id] = std::move(migrate_data.waiting_joins);
  join_tracker_[migrate_meta.partition_id] = std::move(migrate_data.join_tracker);
  num_local_join_part_ += 1;

  // handle buffered request
  LOG(INFO) << "[MigrateDone] part to migrate: " << migrate_meta.partition_id 
    <<", buffered_requests_ size: " << buffered_requests_[migrate_meta.partition_id].size();
  for (auto& request: buffered_requests_[migrate_meta.partition_id]) {
    CHECK_EQ(request.meta.part_id, migrate_meta.partition_id);
    if (map_collection_id_ == join_collection_id_
        && request.meta.version >= map_versions_[request.meta.part_id]) {
      pending_joins_[request.meta.part_id][request.meta.version].push_back(request);
    } else {
      waiting_joins_[request.meta.part_id].push_back(request);
    }
  }
  buffered_requests_[migrate_meta.partition_id].clear();

  auto& func = controller_->engine_elem_.function_store
      ->GetCreatePart(migrate_meta.collection_id);
  auto p = func();
  p->FromBin(bin2);  // now I serialize in the controller thread
  LOG(INFO) << "[MigrateDone] receive migrate partition in dest: " << migrate_meta.DebugString();
  CHECK(!controller_->engine_elem_.partition_manager->Has(migrate_meta.collection_id, migrate_meta.partition_id));
  controller_->engine_elem_.partition_manager->Insert(migrate_meta.collection_id, migrate_meta.partition_id, std::move(p));

  if (map_collection_id_ == join_collection_id_) {
    TryRunSomeMaps();
  }
  TryRunWaitingJoins(migrate_meta.partition_id);

  SArrayBinStream bin;
  ControllerMsg ctrl;
  ctrl.flag = ControllerMsg::Flag::kFinishMigrate;
  ctrl.version = -1;
  ctrl.node_id = -1;
  ctrl.plan_id = plan_id_;
  ctrl.part_id = migrate_meta.partition_id;
  bin << ctrl;
  SendMsgToScheduler(bin);
}

void PlanController::MigratePartitionStartMigrateMapOnly(MigrateMeta migrate_meta) {
  CHECK_EQ(migrate_meta.collection_id, map_collection_id_) << "only consider map_collection first";
  CHECK_NE(migrate_meta.collection_id, join_collection_id_) << "only consider map_collection first";
  CHECK(map_versions_.find(migrate_meta.partition_id) != map_versions_.end());
  
  // version
  int map_version = map_versions_[migrate_meta.partition_id];
  SArrayBinStream bin1;
  bin1 << map_version;

  // partition
  CHECK(controller_->engine_elem_.partition_manager->Has(
    migrate_meta.collection_id, migrate_meta.partition_id)) << migrate_meta.collection_id << " " <<  migrate_meta.partition_id;
  auto part = controller_->engine_elem_.partition_manager->Get(
    migrate_meta.collection_id, migrate_meta.partition_id);
  SArrayBinStream bin2;
  part->ToBin(bin2);  // serialize

  // reset the flag
  migrate_meta.flag = MigrateMeta::MigrateFlag::kReceiveMapOnly;

  // send
  Message msg;
  msg.meta.sender = controller_->engine_elem_.node.id;
  msg.meta.recver = GetControllerActorQid(migrate_meta.to_id);
  msg.meta.flag = Flag::kOthers;
  SArrayBinStream ctrl_bin, plan_bin, ctrl2_bin;
  ctrl_bin << ControllerFlag::kMigratePartition;
  plan_bin << plan_id_;
  ctrl2_bin << migrate_meta;
  msg.AddData(ctrl_bin.ToSArray());
  msg.AddData(plan_bin.ToSArray());
  msg.AddData(ctrl2_bin.ToSArray());
  msg.AddData(bin1.ToSArray());
  msg.AddData(bin2.ToSArray());
  controller_->engine_elem_.sender->Send(std::move(msg));
  LOG(INFO) << "[Migrate] migrating: " << migrate_meta.DebugString();
}

void PlanController::MigratePartitionReceiveMapOnly(Message msg) {
  CHECK_EQ(msg.data.size(), 5);
  SArrayBinStream ctrl2_bin, bin1, bin2;
  ctrl2_bin.FromSArray(msg.data[2]);
  bin1.FromSArray(msg.data[3]);
  bin2.FromSArray(msg.data[4]);
  MigrateMeta migrate_meta;
  ctrl2_bin >> migrate_meta;

  // version
  int map_version;
  bin1 >> map_version;

  auto& func = controller_->engine_elem_.function_store
      ->GetCreatePart(migrate_meta.collection_id);
  auto p = func();
  p->FromBin(bin2);  // now I serialize in the controller thread

  RunMap(migrate_meta.partition_id, map_version, p);
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

