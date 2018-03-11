#include "core/worker/controller.hpp"

#include "core/scheduler/control.hpp"

#include "core/partition/abstract_map_progress_tracker.hpp"

namespace xyz {

struct FakeTracker : public AbstractMapProgressTracker {
  virtual void Report(int) override {}
};

void Controller::Setup(SArrayBinStream bin) {
  SpecWrapper spec;
  bin >> spec;

  type_ = spec.type;
  CHECK(type_ == SpecWrapper::Type::kMapJoin
       || type_ == SpecWrapper::Type::kMapWithJoin);
  auto* p = static_cast<MapJoinSpec*>(spec.spec.get());
  map_collection_id_ = p->map_collection_id;
  join_collection_id_ = p->join_collection_id;
  plan_id_ = spec.id;
  num_upstream_part_ = engine_elem_.collection_map->GetNumParts(map_collection_id_);
  num_local_join_part_ = engine_elem_.partition_manager->GetNumLocalParts(join_collection_id_);
  num_local_map_part_ = engine_elem_.partition_manager->GetNumLocalParts(map_collection_id_);
  min_version_ = 0;
  staleness_ = 0;
  min_map_version_ = 0;
  min_join_version_ = 0;
  map_versions_.clear();
  join_versions_.clear();
  join_tracker_.clear();
  pending_joins_.clear();
  waiting_joins_.clear();

  auto parts = engine_elem_.partition_manager->Get(map_collection_id_);
  for (auto& part : parts) {
    map_versions_[part->part_id] = 0;
  }
  parts = engine_elem_.partition_manager->Get(join_collection_id_);
  for (auto& part : parts) {
    join_versions_[part->part_id] = 0;
  }

  SArrayBinStream reply_bin;
  ControllerMsg ctrl;
  ctrl.flag = ControllerMsg::Flag::kSetup;
  ctrl.node_id = engine_elem_.node.id;
  reply_bin << ctrl;
  SendMsgToScheduler(reply_bin);
}

void Controller::Process(Message msg) {
  CHECK_EQ(msg.data.size(), 2); // cmd, content
  SArrayBinStream ctrl_bin;
  SArrayBinStream bin;
  ctrl_bin.FromSArray(msg.data[0]);
  bin.FromSArray(msg.data[1]);
  ControllerFlag flag;
  ctrl_bin >> flag;

  switch (flag) {
  case ControllerFlag::kSetup: {
    Setup(bin);
    break;
  }
  case ControllerFlag::kStart: {
    StartPlan();
    break;
  }
  case ControllerFlag::kFinishMap: {
    FinishMap(bin);
    break;
  }
  case ControllerFlag::kFinishJoin: {
    FinishJoin(bin);
    break;
  }
  case ControllerFlag::kUpdateVersion: {
    UpdateVersion(bin);
    break;
  }
  case ControllerFlag::kReceiveJoin: {
    ReceiveJoin(msg);
    break;
  }
  default: CHECK(false);
  }
}

void Controller::StartPlan() {
  TryRunSomeMaps();
}


void Controller::UpdateVersion(SArrayBinStream bin) {
  int new_version;
  bin >> new_version;
  if (new_version == -1) {
    SArrayBinStream bin;
    ControllerMsg ctrl;
    ctrl.flag = ControllerMsg::Flag::kFinish;
    ctrl.version = min_map_version_;
    ctrl.node_id = engine_elem_.node.id;
    bin << ctrl;
    SendMsgToScheduler(bin);
  }
  CHECK_EQ(new_version, min_version_+1);
  min_version_ = new_version;
  TryRunSomeMaps();
}

void Controller::FinishMap(SArrayBinStream bin) {
  int part_id;
  bin >> part_id;
  int last_version = map_versions_[part_id];
  running_maps_.erase(part_id);
  map_versions_[part_id] += 1;
  TryUpdateMapVersion();

  if (map_collection_id_ == join_collection_id_) {
    if (!pending_joins_[part_id][last_version].empty()) {
      waiting_joins_[part_id] = std::move(pending_joins_[part_id][last_version]);
      pending_joins_[part_id].erase(last_version);
      bool run = TryRunWaitingJoins(part_id);
      // if (run) {
      //   return;
      // }
    }
  }
  // select other maps
  TryRunSomeMaps();
}

void Controller::TryRunSomeMaps() {
  // if there is no map partitions
  if (map_versions_.empty()) {
    min_map_version_ += 1;
    SendUpdateMapVersionToScheduler();
  }
  // if there is no join partition
  if (join_versions_.empty()) {
    min_join_version_ += 1;
    SendUpdateJoinVersionToScheduler();
  }
  if (map_versions_.empty() || join_versions_.empty()) {
    return;
  }

  for (auto kv : map_versions_) {
    if (IsMapRunnable(kv.first)) {
      RunMap(kv.first, kv.second);
    }
  }
}

bool Controller::IsMapRunnable(int part_id) {
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
  if (version <= min_version_ + staleness_) {
    return true;
  } else {
    return false;
  }
}

bool Controller::TryRunWaitingJoins(int part_id) {
  // see whether there is any waiting joins
  auto& joins = waiting_joins_[part_id];
  if (!joins.empty()) {
    VersionedJoinMeta meta = joins.front();
    joins.pop_front();
    RunJoin(meta);
    return true;
  }
  return false;
}

void Controller::FinishJoin(SArrayBinStream bin) {
  int part_id, upstream_part_id, version;
  bin >> part_id >> upstream_part_id >> version;
  running_joins_.erase(part_id);

  join_tracker_[part_id][version].insert(upstream_part_id);
  if (join_tracker_[part_id][version].size() == num_upstream_part_) {
    join_tracker_[part_id].erase(version);
    join_versions_[part_id] += 1;
    TryUpdateJoinVersion();
  }
  bool run = TryRunWaitingJoins(part_id);
  if (run) {
    return;
  }
}

void Controller::TryUpdateMapVersion() {
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


void Controller::TryUpdateJoinVersion() {
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

void Controller::SendUpdateMapVersionToScheduler() {
  LOG(INFO) << "[Controller] update map version: " << min_map_version_
      << ", on " << engine_elem_.node.id;
  SArrayBinStream bin;
  ControllerMsg ctrl;
  ctrl.flag = ControllerMsg::Flag::kMap;
  ctrl.version = min_map_version_;
  ctrl.node_id = engine_elem_.node.id;
  bin << ctrl;
  SendMsgToScheduler(bin);
}
void Controller::SendUpdateJoinVersionToScheduler() {
  LOG(INFO) << "[Controller] update join version: " << min_join_version_
      << ", on " << engine_elem_.node.id;
  SArrayBinStream bin;
  ControllerMsg ctrl;
  ctrl.flag = ControllerMsg::Flag::kJoin;
  ctrl.version = min_join_version_;
  ctrl.node_id = engine_elem_.node.id;
  bin << ctrl;
  SendMsgToScheduler(bin);
}

void Controller::RunMap(int part_id, int version) {
  CHECK(running_maps_.find(part_id) == running_maps_.end());
  running_maps_.insert(part_id);

  engine_elem_.executor->Add([this, part_id, version]() {
    // 0. get partition
    CHECK(engine_elem_.partition_manager->Has(map_collection_id_, part_id));
    auto p = engine_elem_.partition_manager->Get(map_collection_id_, part_id)->partition;
    auto pt = std::make_shared<FakeTracker>();
    // 1. map
    std::shared_ptr<AbstractMapOutput> map_output;
    if (type_ == SpecWrapper::Type::kMapJoin) {
      auto& map = engine_elem_.function_store->GetMap(plan_id_);
      map_output = map(p, pt); 
    } else if (type_ == SpecWrapper::Type::kMapWithJoin){
      auto& mapwith = engine_elem_.function_store->GetMapWith(plan_id_);
      map_output = mapwith(p, engine_elem_.fetcher, pt); 
    } else {
      CHECK(false);
    }
    // 2. serialize
    auto bins = map_output->Serialize();
    // 3. add to intermediate_store
    for (int i = 0; i < bins.size(); ++ i) {
      Message msg;
      msg.meta.sender = Qid();
      CHECK(engine_elem_.collection_map);
      msg.meta.recver = GetControllerActorQid(engine_elem_.collection_map->Lookup(join_collection_id_, i));
      msg.meta.flag = Flag::kOthers;
      SArrayBinStream ctrl_bin, ctrl2_bin;
      ctrl_bin << ControllerFlag::kReceiveJoin;

      VersionedShuffleMeta meta;
      meta.plan_id = plan_id_;
      meta.collection_id = join_collection_id_;
      meta.upstream_part_id = part_id;
      meta.part_id = i;
      meta.version = version;
      ctrl2_bin << meta;

      msg.AddData(ctrl_bin.ToSArray());
      msg.AddData(ctrl2_bin.ToSArray());
      msg.AddData(bins[i].ToSArray());
      engine_elem_.intermediate_store->Add(msg);
    }
    Message msg;
    msg.meta.sender = 0;
    msg.meta.recver = 0;
    msg.meta.flag = Flag::kOthers;
    SArrayBinStream ctrl_bin, bin;
    ctrl_bin << ControllerFlag::kFinishMap;
    bin << part_id;
    msg.AddData(ctrl_bin.ToSArray());
    msg.AddData(bin.ToSArray());
    GetWorkQueue()->Push(msg);
  });
}

void Controller::ReceiveJoin(Message msg) {
  CHECK_EQ(msg.data.size(), 3);
  SArrayBinStream ctrl2_bin, bin;
  ctrl2_bin.FromSArray(msg.data[1]);
  bin.FromSArray(msg.data[2]);
  VersionedShuffleMeta meta;
  ctrl2_bin >> meta;

  VersionedJoinMeta join_meta;
  join_meta.meta = meta;
  join_meta.bin = bin;
  // TODO: how to control the version!
  if (map_collection_id_ == join_collection_id_) {
    if (meta.version >= map_versions_[meta.part_id]) {
      pending_joins_[meta.part_id][meta.part_id].push_back(join_meta);
      return;
    }
  }

  if (running_joins_.find(meta.part_id) != running_joins_.end()) {
    // someone is joining this part
    waiting_joins_[meta.part_id].push_back(join_meta);
    return;
  }
  if (map_collection_id_ == join_collection_id_ 
          && running_maps_.find(meta.part_id) != running_maps_.end()) {
    // someone is mapping this part
    waiting_joins_[meta.part_id].push_back(join_meta);
    return;
  }
  RunJoin(join_meta);
}

void Controller::RunJoin(VersionedJoinMeta meta) {
  running_joins_.insert(meta.meta.part_id);
  engine_elem_.executor->Add([this, meta]() {
    auto& join_func = engine_elem_.function_store->GetJoin(plan_id_);
    CHECK(engine_elem_.partition_manager->Has(join_collection_id_, meta.meta.part_id));
    auto p = engine_elem_.partition_manager->Get(join_collection_id_, meta.meta.part_id)->partition;
    join_func(p, meta.bin);

    Message msg;
    msg.meta.sender = 0;
    msg.meta.recver = 0;
    msg.meta.flag = Flag::kOthers;
    SArrayBinStream ctrl_bin, bin;
    ctrl_bin << ControllerFlag::kFinishJoin;
    bin << meta.meta.part_id << meta.meta.upstream_part_id << meta.meta.version;
    msg.AddData(ctrl_bin.ToSArray());
    msg.AddData(bin.ToSArray());
    GetWorkQueue()->Push(msg);
  });
}

void Controller::SendMsgToScheduler(SArrayBinStream bin) {
  Message msg;
  msg.meta.sender = Qid();
  msg.meta.recver = 0;
  msg.meta.flag = Flag::kOthers;
  SArrayBinStream ctrl_bin;
  ctrl_bin << ScheduleFlag::kControl;
  msg.AddData(ctrl_bin.ToSArray());
  msg.AddData(bin.ToSArray());
  engine_elem_.sender->Send(std::move(msg));
}

}  // namespace

