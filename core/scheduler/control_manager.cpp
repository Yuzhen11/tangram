#include "core/scheduler/control_manager.hpp"

#include "core/queue_node_map.hpp"

#include "base/color.hpp"

#include <algorithm>

namespace xyz {

void ControlManager::Control(SArrayBinStream bin) {
  ControllerMsg ctrl;
  bin >> ctrl;
  VLOG(2) << "[ControlManager] ctrl: " << ctrl.DebugString();
  if (ctrl.flag == ControllerMsg::Flag::kSetup) {
    is_setup_[ctrl.plan_id].insert(ctrl.node_id);
    if (is_setup_[ctrl.plan_id].size() == elem_->nodes.size()) {
      LOG(INFO) << "[ControlManager] Setup all nodes, startPlan: " << ctrl.plan_id;
      SArrayBinStream reply_bin;
      SendToAllControllers(ControllerFlag::kStart, ctrl.plan_id, reply_bin);
      version_time_[ctrl.plan_id].push_back(std::chrono::system_clock::now());  

      // set the map_versions_ time
      start_time_ = std::chrono::system_clock::now();
      for (auto& node : elem_->nodes) {
        map_versions_[ctrl.plan_id][node.second.node.id].first = 0;
        map_versions_[ctrl.plan_id][node.second.node.id].second = std::chrono::system_clock::now();
      }
    }
  } else if (ctrl.flag == ControllerMsg::Flag::kMap) {
    CHECK_EQ(map_versions_[ctrl.plan_id][ctrl.node_id].first + 1, ctrl.version) << "update version 1 every time? ";
    map_versions_[ctrl.plan_id][ctrl.node_id].first = ctrl.version;
    map_versions_[ctrl.plan_id][ctrl.node_id].second = std::chrono::system_clock::now();
    TryUpdateVersion(ctrl.plan_id);
#ifdef WITH_LB
    // TODO: add logic here
    if (migrate_control_) {
      Migrate(ctrl.plan_id);
      migrate_control_ = false;
    }
    // TrySpeculativeMap(ctrl.plan_id);
#endif
  } else if (ctrl.flag == ControllerMsg::Flag::kJoin) {
    // CHECK_EQ(join_versions_[ctrl.plan_id][ctrl.node_id] + 1, ctrl.version) << "update version 1 every time? ";
    join_versions_[ctrl.plan_id][ctrl.node_id] = ctrl.version;
    TryUpdateVersion(ctrl.plan_id);
  } else if (ctrl.flag == ControllerMsg::Flag::kFinish) {
    is_finished_[ctrl.plan_id].insert(ctrl.node_id);
    if (is_finished_[ctrl.plan_id].size() == elem_->nodes.size()) {
      SArrayBinStream reply_bin;
      reply_bin << ctrl.plan_id;
      ToScheduler(elem_, ScheduleFlag::kFinishPlan, reply_bin);

      // print version intervals
      for (int i = 0; i < version_time_[ctrl.plan_id].size()-1; i++) {
        std::chrono::duration<double> duration = version_time_[ctrl.plan_id].at(i+1) - version_time_[ctrl.plan_id].at(i);
        LOG(INFO) << "[ControlManager] version interval: " << "(" << i << "->" << i+1 << ") " << duration.count();
      }
    }
  }
}

void ControlManager::PrintMapVersions(int plan_id) {
  std::stringstream ss;
  for (auto& v: map_versions_[plan_id]) {
    ss << "node: " << v.first 
       << ", version: " << v.second.first 
       << ", time: " << static_cast<std::chrono::duration<double>>(v.second.second-start_time_).count()
       << "s\n";
  }
  LOG(INFO) << ss.str();
}

// TODO: a fake method with hardcode migrate information now
void ControlManager::Migrate(int plan_id) {
  auto* mapjoin_spec = static_cast<MapJoinSpec*>(specs_[plan_id].spec.get());
  // update collection_map
  auto& collection_view = elem_->collection_map->Get(mapjoin_spec->join_collection_id);
  auto& part_to_node = collection_view.mapper.Mutable();
  CHECK_LT(4, part_to_node.size());
  part_to_node[4] = 1;

  MigrateMeta2 migrate_meta;
  migrate_meta.flag = MigrateMeta2::MigrateFlag::kStartMigrate;
  migrate_meta.plan_id = plan_id;
  migrate_meta.collection_id = mapjoin_spec->join_collection_id;
  migrate_meta.partition_id = 4;
  migrate_meta.from_id = 5;
  migrate_meta.to_id = 1;
  migrate_meta.num_nodes = elem_->nodes.size();
  SArrayBinStream bin;
  bin << migrate_meta << collection_view;
  SendToAllControllers(ControllerFlag::kMigratePartition, plan_id, bin);
  join_versions_[plan_id][1] -= 1;
}

void ControlManager::TrySpeculativeMap(int plan_id) {
  PrintMapVersions(plan_id);
  auto* mapjoin_spec = static_cast<MapJoinSpec*>(specs_[plan_id].spec.get());
  if (mapjoin_spec->map_collection_id == mapjoin_spec->join_collection_id) {
    // do not handle this case now.
    return;
  }
  int staleness = mapjoin_spec->staleness;
  int fastest_version = versions_[plan_id] + staleness + 1;

  std::vector<int> fast_nodes;
  // identify nodes with fastest version
  for (auto v : map_versions_[plan_id]) {
    if (v.second.first == fastest_version) {
      fast_nodes.push_back(v.first);
    }
  }
  if (fast_nodes.empty()) {
    return;
  }
  // TODO: now I use the first one
  int fastest_node = fast_nodes[0];

  // identify the slowest node
  auto min_time_node = std::min_element(map_versions_[plan_id].begin(), map_versions_[plan_id].end(), 
          [](std::pair<int, std::pair<int, Timepoint>> a, std::pair<int, std::pair<int, Timepoint>> b) {
            return a.second.second < b.second.second;
          });
  int slowest_node = min_time_node->first;
  if (slowest_node == fastest_node) {
    return;
  }
  LOG(INFO) << "Identify fast node: " << fastest_node << ", slow node: " << slowest_node;

  MigrateMeta meta;
  meta.plan_id = plan_id;
  meta.collection_id = mapjoin_spec->map_collection_id;
  meta.partition_id = -1;
  meta.from_id = slowest_node;
  meta.to_id = fastest_node;
  meta.current_map_version = -1;

  SArrayBinStream bin;
  bin << meta;
  SendToController(slowest_node, ControllerFlag::kRequestPartition, plan_id, bin);
}

void ControlManager::TryUpdateVersion(int plan_id) {
  int current_version = versions_[plan_id];
  for (auto v: map_versions_[plan_id]) {
    if (current_version == v.second.first) {
      return;
    }
  }
  for (auto v: join_versions_[plan_id]) {
    if (current_version == v.second) {
      return;
    }
  }
  versions_[plan_id] ++;
  //record time 
  version_time_[plan_id].push_back(std::chrono::system_clock::now());
  
  if (versions_[plan_id] == expected_versions_[plan_id]) {
    LOG(INFO) << "[ControlManager] Finish versions: " << versions_[plan_id] << " for plan " << plan_id;
    // send finish
    SArrayBinStream bin;
    bin << int(-1);
    SendToAllControllers(ControllerFlag::kUpdateVersion, plan_id, bin);
  } else {
    LOG(INFO) << "[ControlManager] Update min version: " << versions_[plan_id] << " for plan " << plan_id; 
    // update version
    SArrayBinStream bin;
    bin << versions_[plan_id];
    SendToAllControllers(ControllerFlag::kUpdateVersion, plan_id, bin);
  }
}

void ControlManager::RunPlan(SpecWrapper spec) {
  CHECK(spec.type == SpecWrapper::Type::kMapJoin
       || spec.type == SpecWrapper::Type::kMapWithJoin);

  int plan_id = spec.id;
  specs_[plan_id] = spec;

  is_setup_[plan_id].clear();
  is_finished_[plan_id].clear();
  map_versions_[plan_id].clear();
  join_versions_[plan_id].clear();
  versions_[plan_id] = 0;
  expected_versions_[plan_id] = static_cast<MapJoinSpec*>(spec.spec.get())->num_iter;
  CHECK_NE(expected_versions_[plan_id], 0);
  // LOG(INFO) << "[ControlManager] Start a plan num_iter: " << expected_versions_[plan_id]; 

  for (auto& node : elem_->nodes) {
    map_versions_[plan_id][node.second.node.id].first = 0;
    join_versions_[plan_id][node.second.node.id] = 0;
  }
  SArrayBinStream bin;
  bin << spec;
  SendToAllControllers(ControllerFlag::kSetup, plan_id, bin);
}

void ControlManager::SendToController(int node_id, ControllerFlag flag, int plan_id, SArrayBinStream bin) {
  SArrayBinStream ctrl_bin, plan_bin;
  ctrl_bin << flag;
  plan_bin << plan_id;
  Message msg;
  msg.meta.sender = 0;
  msg.meta.recver = GetControllerActorQid(node_id);
  msg.meta.flag = Flag::kOthers;
  msg.AddData(ctrl_bin.ToSArray());
  msg.AddData(plan_bin.ToSArray());
  msg.AddData(bin.ToSArray());
  elem_->sender->Send(std::move(msg));
}

void ControlManager::SendToAllControllers(ControllerFlag flag, int plan_id, SArrayBinStream bin) {
  SArrayBinStream ctrl_bin, plan_bin;
  ctrl_bin << flag;
  plan_bin << plan_id;
  for (auto& node : elem_->nodes) {
    Message msg;
    msg.meta.sender = 0;
    msg.meta.recver = GetControllerActorQid(node.second.node.id);
    msg.meta.flag = Flag::kOthers;
    msg.AddData(ctrl_bin.ToSArray());
    msg.AddData(plan_bin.ToSArray());
    msg.AddData(bin.ToSArray());
    elem_->sender->Send(std::move(msg));
  }
}

int ControlManager::GetCurVersion(int plan_id) {
  return versions_[plan_id];
}

} // namespace xyz
