#include "core/scheduler/control_manager.hpp"

#include "core/queue_node_map.hpp"

#include "base/color.hpp"

#include <algorithm>
//#define WITH_LB

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
      SendToAllControllers(elem_, ControllerFlag::kStart, ctrl.plan_id, reply_bin);
      version_time_[ctrl.plan_id].push_back(std::chrono::system_clock::now());  

      Init(ctrl.plan_id);
    }
  } else if (ctrl.flag == ControllerMsg::Flag::kMap) {
    HandleUpdateMapVersion(ctrl);
#ifdef WITH_LB
    // TODO: add logic here
    // for pr
    // for pr() in test_lb.cpp specifically
    //if (specs_[ctrl.plan_id].id == 3 && 
    //    ctrl.node_id == 1 &&
    //    map_node_versions_[ctrl.plan_id][ctrl.node_id].first == 1 && 
    //    migrate_control_) {  
    //  Migrate(ctrl.plan_id, 2, 1, 1);
    //  migrate_control_ = false;
    //}
    // // for mr
    // if (migrate_control_) {
    //   MigrateMapOnly(ctrl.plan_id, 5, 1, 4);
    //   migrate_control_ = false;
    // }
    // TrySpeculativeMap(ctrl.plan_id);
  
    if (migrate_control && specs_[ctrl.plan_id].name == "pagerank main logic") {
      std::vector<std::tuple<int, int, int>> meta;
      for (int i = 0; i < 100; i = i + 5) {
        int to_id = (i / 5) % 4 + 2;
        int from_id = 1;
        meta.push_back(std::make_tuple(from_id, to_id, i));
      }
      PreBatchMigrate(ctrl.plan_id, meta);
      migrate_control = false;
    }

    //if (specs_[ctrl.plan_id].name == "pagerank main logic") { 
    //  TryMigrate(ctrl.plan_id);
    //}
#endif
  } else if (ctrl.flag == ControllerMsg::Flag::kJoin) {
    HandleUpdateJoinVersion(ctrl);
  } else if (ctrl.flag == ControllerMsg::Flag::kFinish) {
    is_finished_[ctrl.plan_id].insert(ctrl.node_id);
    if (is_finished_[ctrl.plan_id].size() == elem_->nodes.size()) {
      if (versions_[ctrl.plan_id] == expected_versions_[ctrl.plan_id]) {
        // print version intervals
        for (int i = 0; i < version_time_[ctrl.plan_id].size()-1; i++) {
          std::chrono::duration<double> duration = version_time_[ctrl.plan_id].at(i+1) - version_time_[ctrl.plan_id].at(i);
          LOG(INFO) << "[ControlManager] version interval: " << "(" << i << "->" << i+1 << ") " << duration.count();
        }
      } else {
        LOG(INFO) << "[ControlManager] Abort Plan: " << ctrl.plan_id;
      }

      CHECK(callbacks_.find(ctrl.plan_id) != callbacks_.end());
      callbacks_[ctrl.plan_id]();
      callbacks_.erase(ctrl.plan_id);
    }
  } else if (ctrl.flag == ControllerMsg::Flag::kFinishMigrate) {
    migrate_time[ctrl.part_id].second = std::chrono::system_clock::now();
    std::chrono::duration<double> duration = migrate_time[ctrl.part_id].second - migrate_time[ctrl.part_id].first;
    LOG(INFO) << "[ControlManager] migrate part " << ctrl.part_id << " done, migrate time: "
      << duration.count();
  } else {
    CHECK(false) << ctrl.DebugString();
  }
}

void ControlManager::TryMigrate(int plan_id){
  auto* mapjoin_spec = static_cast<MapJoinSpec*>(specs_[plan_id].spec.get());
  if (mapjoin_spec->map_collection_id == mapjoin_spec->join_collection_id) {
    int max_version = versions_[plan_id] + mapjoin_spec->staleness + 1;
    std::map<int, std::vector<int>> nodes; // version, node ids
    for (auto node : map_node_versions_[plan_id]) {
      nodes[node.second.first].push_back(node.first);
    }
    std::vector<int> fast_nodes = nodes[max_version];
    std::vector<int> slow_nodes;
    for (int i = versions_[plan_id]; i < max_version; i++) {
      for (auto node : nodes[i]) {
        slow_nodes.push_back(node);
      }
      break; //only migrate for the min version
    }
    auto& collection_view = elem_->collection_map->Get(mapjoin_spec->map_collection_id);
   
    //TODO: better schedule migration
    std::vector<std::tuple<int, int, int>> meta;
    auto iter1 = fast_nodes.begin();
    auto iter2 = slow_nodes.begin();
    while (iter1 != fast_nodes.end() && iter2 != slow_nodes.end()) {
      std::vector<int> part_ids = collection_view.mapper.GetNodeParts(*iter2);
      std::map<int, std::vector<int>> version; // version, part_ids
      for (int part_id : part_ids) {
        version[map_part_versions_[plan_id][part_id].first].push_back(part_id);
      }
      for (int i = versions_[plan_id]; i < max_version; i++) {
        if (!version[i].empty()) {
          int part_id = version[i].at(0);
          if (std::find(parts_migrated[part_id].begin(), parts_migrated[part_id].end(), map_part_versions_[plan_id][part_id].first)
              != parts_migrated[part_id].end()) continue;
          LOG(INFO)<<"[ControlManager Migrate] plan "<<plan_id<<" version "<<versions_[plan_id]
            <<", from node "<< *iter2 <<" version "<< map_node_versions_[plan_id][*iter2].first
            <<", to node "<< *iter1 <<" version "<< map_node_versions_[plan_id][*iter1].first
            <<", migrate part "<< part_id <<" version "<< map_part_versions_[plan_id][part_id].first;
          
          meta.push_back(std::make_tuple(*iter2, *iter1, part_id));
          parts_migrated[part_id].push_back(map_part_versions_[plan_id][part_id].first);
          iter1++;
          //return; //only migrate one partition at one time
        }
        break; //only migrate for the min version
      }
      iter2++;
    }
    PreBatchMigrate(plan_id, meta);
  }
}

void ControlManager::HandleUpdateMapVersion(ControllerMsg ctrl) {
  auto& part_versions = map_part_versions_[ctrl.plan_id];
  auto& node_versions = map_node_versions_[ctrl.plan_id];
  auto& node_count = map_node_count_[ctrl.plan_id];
  auto* mapjoin_spec = static_cast<MapJoinSpec*>(specs_[ctrl.plan_id].spec.get());
  auto& collection_view = elem_->collection_map->Get(mapjoin_spec->map_collection_id);
  auto& part_to_node_map = collection_view.mapper.Get();

  CHECK_EQ(part_versions[ctrl.part_id].first + 1, ctrl.version) << "version updated by 1 every time";
  part_versions[ctrl.part_id].first = ctrl.version;
  part_versions[ctrl.part_id].second = std::chrono::system_clock::now();

  int node_id = part_to_node_map[ctrl.part_id];
  if (node_versions[node_id].first == ctrl.version - 1) {
    CHECK_GT(node_count[node_id], 0);
    node_count[node_id] -= 1;
    if (node_count[node_id] == 0) {
      node_versions[node_id].first += 1;
      node_versions[node_id].second = std::chrono::system_clock::now();

      // LOG(INFO) << DebugVersions(ctrl.plan_id);
      LOG(INFO) << "[ControlManager::HandleUpdateMapVersion] node: " << node_id << ", map version: " << node_versions[node_id].first;
      // update node_count to next version
      node_count[node_id] = 0;
      for (auto& pv : part_versions) {
        if (part_to_node_map[pv.first] == node_id && pv.second.first == node_versions[node_id].first) {
          node_count[node_id] += 1;
        }
      }
    }
  }
}

void ControlManager::HandleUpdateJoinVersion(ControllerMsg ctrl) {
  auto& part_versions = join_part_versions_[ctrl.plan_id];
  auto& node_versions = join_node_versions_[ctrl.plan_id];
  auto& node_count = join_node_count_[ctrl.plan_id];
  auto* mapjoin_spec = static_cast<MapJoinSpec*>(specs_[ctrl.plan_id].spec.get());
  auto& collection_view = elem_->collection_map->Get(mapjoin_spec->join_collection_id);
  auto& part_to_node_map = collection_view.mapper.Get();

  CHECK_EQ(part_versions[ctrl.part_id].first + 1, ctrl.version) << "version updated by 1 every time";
  part_versions[ctrl.part_id].first = ctrl.version;
  part_versions[ctrl.part_id].second = std::chrono::system_clock::now();

  int node_id = part_to_node_map[ctrl.part_id];
  if (node_versions[node_id].first == ctrl.version - 1) {
    CHECK_GT(node_count[node_id], 0);
    node_count[node_id] -= 1;
    if (node_count[node_id] == 0) {
      node_versions[node_id].first += 1;
      node_versions[node_id].second = std::chrono::system_clock::now();

      // LOG(INFO) << DebugVersions(ctrl.plan_id);
      LOG(INFO) << "node: " << node_id << ", join version: " << node_versions[node_id].first;
      // update node_count to next version
      node_count[node_id] = 0;
      for (auto& pv : part_versions) {
        if (part_to_node_map[pv.first] == node_id && pv.second.first == node_versions[node_id].first) {
          node_count[node_id] += 1;
        }
      }
      // try update version
      bool update_join_version = true;
      for (auto& node_version : node_versions) {
        if (node_count[node_version.first] == 0) {  // no join part there
          continue;
        }
        if (node_version.second.first == versions_[ctrl.plan_id]) {
          update_join_version = false;
          break;
        }
      }
      if (update_join_version) {
        UpdateVersion(ctrl.plan_id);
      }
    }
  }
}

void ControlManager::PreBatchMigrate(int plan_id, std::vector<std::tuple<int, int, int>> meta) {
  for (std::tuple<int, int, int> submeta : meta) {
    int part_id = std::get<2>(submeta);
    migrate_time[part_id].first = std::chrono::system_clock::now();
  }

  bool is_mapwithjoin = false;//TODO: automatically detect
  auto* mapjoin_spec = static_cast<MapJoinSpec*>(specs_[plan_id].spec.get());
  if (is_mapwithjoin &&
      mapjoin_spec->map_collection_id == mapjoin_spec->join_collection_id
      ) {
    auto* mapjoin_spec = static_cast<MapWithJoinSpec*>(specs_[plan_id].spec.get());
    if (mapjoin_spec->map_collection_id != mapjoin_spec->with_collection_id) {
      // no need to migrate with_part if map collection equals with collection
      auto& collection_view = elem_->collection_map->Get(mapjoin_spec->with_collection_id);
      auto& part_to_node = collection_view.mapper.Mutable();

      std::string url = collection_status_->GetLastCP(mapjoin_spec->with_collection_id);
      std::vector<int> with_parts;
      for (std::tuple<int, int, int> submeta : meta) {
        int from_id = std::get<0>(submeta);
        int to_id = std::get<1>(submeta);
        int part_id = std::get<2>(submeta);
        CHECK_EQ(from_id, part_to_node[part_id]);
        part_to_node[part_id] = to_id;
        with_parts.push_back(std::get<2>(submeta));
      }
      int with_collection_id = mapjoin_spec->with_collection_id;
      checkpoint_loader_->LoadCheckpointPartial(mapjoin_spec->with_collection_id,
          url, with_parts, [this, plan_id, meta, with_collection_id, part_to_node]() {
        //load checkpoint finished
        //1. update with collection view
        LOG(INFO) << GREEN("[ControlManager::PreBatchMigrate] load checkpoint finished");
        collection_manager_->Update(with_collection_id,
            [this, plan_id, meta, with_collection_id, part_to_node](){
          //update with collection view finished
          //2. send msgs to controller
          LOG(INFO) << GREEN("[ControlManager::PreBatchMigrate] update with collection view finished");
          for (std::tuple<int, int, int> submeta : meta) {
            int to_id = std::get<1>(submeta);
            SArrayBinStream bin;
            bin << submeta;
            SendToController(elem_, to_id,  ControllerFlag::kFinishLoadWith, plan_id, bin);
          }         
        });

      });
    }
  }

  BatchMigrate(plan_id, meta);
}

void ControlManager::BatchMigrate(int plan_id, std::vector<std::tuple<int, int, int>> meta) {
  auto* mapjoin_spec = static_cast<MapJoinSpec*>(specs_[plan_id].spec.get());
  auto& collection_view = elem_->collection_map->Get(mapjoin_spec->join_collection_id);
  auto& part_to_node = collection_view.mapper.Mutable();
  for (std::tuple<int, int, int> submeta : meta) {
    int from_id = std::get<0>(submeta);
    int to_id= std::get<1>(submeta);
    int part_id = std::get<2>(submeta);
    CHECK_LT(part_id, part_to_node.size());
    CHECK_EQ(part_to_node[part_id], from_id) << ", part_id: " << part_id;
    
    //migrate join 
    part_to_node[part_id] = to_id;
    auto current_time = std::chrono::system_clock::now();
    CHECK_EQ(join_part_versions_[plan_id][part_id].first, versions_[plan_id]) << "only migrate for the minimum version";
    // LOG(INFO) << DebugVersions(plan_id);
    // update from_id
    // try to update join_node_versions_ and join_node_count_
    CHECK_EQ(join_part_versions_[plan_id][part_id].first, join_node_versions_[plan_id][from_id].first);
    if (join_node_count_[plan_id][from_id] > 1) {
      join_node_count_[plan_id][from_id] -= 1;
    } else if (join_node_count_[plan_id][from_id] == 1) {
      // 1. find a new min version for from_id
      int new_min = 1000000;
      for (int i = 0; i < part_to_node.size(); ++ i) {
        if (part_to_node[i] == from_id) {
          if (join_part_versions_[plan_id][i].first < new_min) {
            new_min = join_part_versions_[plan_id][i].first;
          }
        }
      }
      // 2. calc count
      int count = 0;
      for (int i = 0; i < part_to_node.size(); ++ i) {
        if (part_to_node[i] == from_id) {
          if (join_part_versions_[plan_id][i].first == new_min) {
            count += 1;
          }
        }
      }
      join_node_count_[plan_id][from_id] = count;
      join_node_versions_[plan_id][from_id].first = new_min;
      join_node_versions_[plan_id][from_id].second = current_time;
      LOG(INFO) << "[ControlManager::Migrate] update join_node_versions_ for node: " << from_id << " to " << new_min << ", min_count: " << count;
    } else {
      CHECK(false);
    }

    // migrate map if map collection equals join collection
    if (mapjoin_spec->map_collection_id == mapjoin_spec->join_collection_id) {
      if (map_node_versions_[plan_id][from_id].first == map_part_versions_[plan_id][part_id].first) {
        // migrating the min version
        if (map_node_count_[plan_id][from_id] > 1) {
          map_node_count_[plan_id][from_id] -= 1;
          LOG(INFO) << "pid, fid, min_count: " << plan_id << ", " << from_id << ", " << map_node_count_[plan_id][from_id];
        } else {
          // 1. find a new version
          int new_min = 1000000;
          for (int i = 0; i < part_to_node.size(); ++ i) {
            if (part_to_node[i] == from_id) {
              if (map_part_versions_[plan_id][i].first < new_min) {
                new_min = map_part_versions_[plan_id][i].first;
              }
            }
          }
          // 2. calc count
          int count = 0;
          for (int i = 0; i < part_to_node.size(); ++ i) {
            if (part_to_node[i] == from_id) {
              if (map_part_versions_[plan_id][i].first == new_min) {
                count += 1;
              }
            }
          }
          map_node_count_[plan_id][from_id] = count;
          map_node_versions_[plan_id][from_id].first = new_min;
          map_node_versions_[plan_id][from_id].second = current_time;
          LOG(INFO) << "update map_node_versions_ for node: " << from_id << " to " << new_min << ", min_count: " << count;
        }
      } else {
        // do nothing
      }
    }

    // update to_id
    if (join_part_versions_[plan_id][part_id].first < join_node_versions_[plan_id][to_id].first) {
      join_node_count_[plan_id][to_id] = 1;
      join_node_versions_[plan_id][to_id].first = join_part_versions_[plan_id][part_id].first;
    } else if (join_part_versions_[plan_id][part_id].first == join_node_versions_[plan_id][to_id].first) {
      join_node_count_[plan_id][to_id] += 1;
    } else {
      // do nothing.
    }

    // for map
    if (mapjoin_spec->map_collection_id == mapjoin_spec->join_collection_id) {
      if (map_part_versions_[plan_id][part_id].first < map_node_versions_[plan_id][to_id].first) {
        map_node_count_[plan_id][to_id] = 1;
        map_node_versions_[plan_id][to_id].first = map_part_versions_[plan_id][part_id].first;
      } else if (map_part_versions_[plan_id][part_id].first == map_node_versions_[plan_id][to_id].first) {
        map_node_count_[plan_id][part_id] += 1;
      } else {
        // do nothing.
      }
    }
    // LOG(INFO) << DebugVersions(plan_id);
    MigrateMeta migrate_meta;
    migrate_meta.flag = MigrateMeta::MigrateFlag::kStartMigrate;
    migrate_meta.plan_id = plan_id;
    migrate_meta.collection_id = mapjoin_spec->join_collection_id;
    migrate_meta.partition_id = part_id;
    migrate_meta.from_id = from_id;
    migrate_meta.to_id = to_id;
    migrate_meta.num_nodes = elem_->nodes.size();
    SArrayBinStream bin;
    bin << migrate_meta << collection_view;
    SendToAllControllers(elem_, ControllerFlag::kMigratePartition, plan_id, bin);
  }
}

void ControlManager::MigrateMapOnly(int plan_id, int from_id, int to_id, int part_id) {
  // some checking
  CHECK(map_part_versions_[plan_id].find(part_id) != map_part_versions_[plan_id].end());
  auto* mapjoin_spec = static_cast<MapJoinSpec*>(specs_[plan_id].spec.get());
  auto& collection_view = elem_->collection_map->Get(mapjoin_spec->map_collection_id);
  CHECK_NE(mapjoin_spec->map_collection_id, mapjoin_spec->join_collection_id);
  auto& part_to_node = collection_view.mapper.Get();
  CHECK_LT(part_id, part_to_node.size());
  CHECK_EQ(part_to_node[part_id], from_id);

  MigrateMeta migrate_meta;
  migrate_meta.flag = MigrateMeta::MigrateFlag::kStartMigrateMapOnly;
  migrate_meta.plan_id = plan_id;
  migrate_meta.collection_id = mapjoin_spec->map_collection_id;
  migrate_meta.partition_id = part_id;
  migrate_meta.from_id = from_id;
  migrate_meta.to_id = to_id;
  migrate_meta.num_nodes = elem_->nodes.size();
  SArrayBinStream bin;
  bin << migrate_meta << collection_view;
  SendToController(elem_, from_id, ControllerFlag::kMigratePartition, plan_id, bin);
}

void ControlManager::UpdateVersion(int plan_id) {
  auto* mapjoin_spec = specs_[plan_id].GetMapJoinSpec();
  // udpate version before AddCP
  // TODO: note that the checkpoint may not be written when AddCP is called
  versions_[plan_id] ++;

  if (mapjoin_spec->checkpoint_interval != 0 
          && versions_[plan_id] % mapjoin_spec->checkpoint_interval == 0
          && versions_[plan_id] != 0) {
    int cp_iter = versions_[plan_id] / mapjoin_spec->checkpoint_interval;
    CHECK(mapjoin_spec->checkpoint_path.size());
    std::string checkpoint_path = mapjoin_spec->checkpoint_path;
    checkpoint_path = checkpoint_path + "/cp-" + std::to_string(cp_iter);
    collection_status_->AddCP(mapjoin_spec->join_collection_id, checkpoint_path);
    LOG(INFO) << "[ControlManager::UpdateVersion] Add checkpoint: " << checkpoint_path;
  }

  //record time 
  version_time_[plan_id].push_back(std::chrono::system_clock::now());
  
  if (versions_[plan_id] == expected_versions_[plan_id]) {
    LOG(INFO) << "[ControlManager] Finish versions: " << versions_[plan_id] << " for plan " << plan_id << " send kTerminatePlan";
    SArrayBinStream dummy_bin;
    SendToAllControllers(elem_, ControllerFlag::kTerminatePlan, plan_id, dummy_bin);
  } else {
    LOG(INFO) << "[ControlManager] Update min version: " << versions_[plan_id] << " for plan " << plan_id; 
    // update version
    SArrayBinStream bin;
    bin << versions_[plan_id];
    SendToAllControllers(elem_, ControllerFlag::kUpdateVersion, plan_id, bin);
  }
}

void ControlManager::AbortPlan(int plan_id, std::function<void()> f) {
  CHECK(callbacks_.find(plan_id) != callbacks_.end());
  callbacks_[plan_id] = f;  // reset the callback

  // terminate it
  SArrayBinStream dummy_bin;
  SendToAllControllers(elem_, ControllerFlag::kTerminatePlan, plan_id, dummy_bin);
}

void ControlManager::RunPlan(SpecWrapper spec, std::function<void()> f) {
  CHECK(spec.type == SpecWrapper::Type::kMapJoin
       || spec.type == SpecWrapper::Type::kMapWithJoin);

  int plan_id = spec.id;
  specs_[plan_id] = spec;

  is_setup_[plan_id].clear();
  is_finished_[plan_id].clear();
  versions_[plan_id] = 0;
  expected_versions_[plan_id] = static_cast<MapJoinSpec*>(spec.spec.get())->num_iter;
  CHECK_NE(expected_versions_[plan_id], 0);
  callbacks_[plan_id] = f;
  // LOG(INFO) << "[ControlManager] Start a plan num_iter: " << expected_versions_[plan_id]; 

  SArrayBinStream bin;
  bin << spec;
  SendToAllControllers(elem_, ControllerFlag::kSetup, plan_id, bin);
}

void ControlManager::Init(int plan_id) {
  start_time_ = std::chrono::system_clock::now();
  for (auto& node : elem_->nodes) {
    map_node_versions_[plan_id][node.second.node.id].first = 0;
    map_node_versions_[plan_id][node.second.node.id].second = start_time_;
    map_node_count_[plan_id][node.second.node.id] = 0;

    join_node_versions_[plan_id][node.second.node.id].first = 0;
    join_node_versions_[plan_id][node.second.node.id].second = start_time_;
    join_node_count_[plan_id][node.second.node.id] = 0;
  }

  auto* mapjoin_spec = static_cast<MapJoinSpec*>(specs_[plan_id].spec.get());
  auto& map_collection_view = elem_->collection_map->Get(mapjoin_spec->map_collection_id);
  auto& map_part_to_node_map = map_collection_view.mapper.Get();
  for (int i = 0; i < map_part_to_node_map.size(); ++ i) {
    map_node_count_[plan_id][map_part_to_node_map[i]] += 1;
    map_part_versions_[plan_id][i].first = 0;
    map_part_versions_[plan_id][i].second = start_time_;
  }

  auto& join_collection_view = elem_->collection_map->Get(mapjoin_spec->join_collection_id);
  auto& join_part_to_node_map = join_collection_view.mapper.Get();
  for (int i = 0; i < join_part_to_node_map.size(); ++ i) {
    join_node_count_[plan_id][join_part_to_node_map[i]] += 1;
    join_part_versions_[plan_id][i].first = 0;
    join_part_versions_[plan_id][i].second = start_time_;
  }
}

int ControlManager::GetCurVersion(int plan_id) {
  return versions_[plan_id];
}

std::string ControlManager::DebugVersions(int plan_id) {
  std::stringstream ss;
  ss << "map_part_versions_: ";
  for (auto& pv : map_part_versions_[plan_id]) {
    ss << pv.first << ": " << pv.second.first << ", ";
  }
  ss << "\nmap_node_versions_: ";
  for (auto& pv : map_node_versions_[plan_id]) {
    ss << pv.first << ": " << pv.second.first << ", ";
  }
  ss << "\nmap_node_count_: ";
  for (auto& pv : map_node_count_[plan_id]) {
    ss << pv.first << ": " << pv.second << ", ";
  }

  ss << "\njoin_part_versions_: ";
  for (auto& pv : join_part_versions_[plan_id]) {
    ss << pv.first << ": " << pv.second.first << ", ";
  }
  ss << "\njoin_node_versions_: ";
  for (auto& pv : join_node_versions_[plan_id]) {
    ss << pv.first << ": " << pv.second.first << ", ";
  }
  ss << "\njoin_node_count_: ";
  for (auto& pv : join_node_count_[plan_id]) {
    ss << pv.first << ": " << pv.second << ", ";
  }
  return ss.str();
}

} // namespace xyz
