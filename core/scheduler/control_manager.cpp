#include "core/scheduler/control_manager.hpp"

#include "core/queue_node_map.hpp"

#include "base/color.hpp"

#include <algorithm>
//#define WITH_LB
//#define WITH_LB_JOIN

//core/scheduler/control_manager.cpp; core/worker/plan_controller.cpp
#define BGCP false

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
    //// for kmeans
    /*
    if (migrate_control && versions_[ctrl.plan_id] == 3 && ctrl.plan_id == 7) {
      migrate_control = false;
      const int from_id = 1;
      const int node_num = 10;
      const int num_parts = 1200;
      const int parts_per_node = num_parts/node_num;
      for (int i = 0; i < parts_per_node; ++ i) {
        int part_id = 0 + i*node_num;
        int to_id = from_id + 1 + i %(node_num-1);
        MigrateMapOnly(ctrl.plan_id, from_id, to_id, part_id);
        LOG(INFO) << "migrating from_id, to_id, part_id: " << from_id << " " << to_id << " " << part_id;
      }
    }
    */
    //// for kmeans
#ifdef WITH_LB_JOIN
    if (migrate_control && 
        specs_[ctrl.plan_id].name == "pagerank main logic" &&
        versions_[ctrl.plan_id] == 6) {//migrate at version x
      std::vector<std::tuple<int, int, int>> meta;
      // NEED TO BE MANUALLY SET
      int from_id = 1;
      int node_num = 20;
      int num_parts = 100;
	  CHECK_EQ(num_parts % node_num, 0);

	  int nodesPerPart = num_parts / node_num;
	  for (int i = 0; i < nodesPerPart; i++) {
		int part_id = 0 + i*node_num;
		int to_id = (from_id + 1 + i%(node_num-1)) % node_num;
		//LOG(INFO) << "to id: " << to_id << " part id: " << part_id << std::endl;
        meta.push_back(std::make_tuple(from_id, to_id, part_id));
	  }
      PreBatchMigrate(ctrl.plan_id, meta);
      migrate_control = false;
    }
#endif
  } else if (ctrl.flag == ControllerMsg::Flag::kFinish) {
    is_finished_[ctrl.plan_id].insert(ctrl.node_id);
    // LOG(INFO) << "finish: " << is_finished_[ctrl.plan_id].size() << ", " << elem_->nodes.size();
    if (is_finished_[ctrl.plan_id].size() == elem_->nodes.size()) {
      if (versions_[ctrl.plan_id] == expected_versions_[ctrl.plan_id]) {
      } else {
        LOG(INFO) << "[ControlManager] Abort Plan: " << ctrl.plan_id;
        version_time_[ctrl.plan_id].push_back(std::chrono::system_clock::now());
      }
      // print version intervals
      for (int i = 0; i < version_time_[ctrl.plan_id].size()-1; i++) {
        std::chrono::duration<double> duration = version_time_[ctrl.plan_id].at(i+1) - version_time_[ctrl.plan_id].at(i);
        LOG(INFO) << "[ControlManager] version interval for plan: " << ctrl.plan_id << ": " << "(" << i << "->" << i+1 << ") " << duration.count();
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
  } else if (ctrl.flag == ControllerMsg::Flag::kFinishCP) {
	ReceiveFinishCP(ctrl.plan_id, ctrl.part_id, ctrl.version);
  } else {
    CHECK(false) << ctrl.DebugString();
  }
}

void ControlManager::ReceiveFinishCP(int plan_id, int part_id, int version) {
  if (versions_[plan_id] == expected_versions_[plan_id]) {
	return;
  }
  cp_count_[plan_id][version].insert(part_id);
  auto* mapupdate_spec = specs_[plan_id].GetMapJoinSpec();
  int collection_id = mapupdate_spec->update_collection_id;
  if (cp_count_[plan_id][version].size() == elem_->collection_map->Get(mapupdate_spec->map_collection_id).num_partition) {
    if (mapupdate_spec->checkpoint_interval != 0) {
      int cp_iter = versions_[plan_id] / mapupdate_spec->checkpoint_interval;
      CHECK(mapupdate_spec->checkpoint_path.size());
      std::string checkpoint_path = mapupdate_spec->checkpoint_path;
      checkpoint_path = checkpoint_path + "/cp-" + std::to_string(cp_iter);
      collection_status_->AddCP(mapupdate_spec->update_collection_id, checkpoint_path);
      LOG(INFO) << RED("[ControlManager::UpdateVersion] BGCP: checkpoint added for collection: "
              + std::to_string(mapupdate_spec->update_collection_id) + ", path: " + checkpoint_path);
    }
  }
}

void ControlManager::TryMigrate(int plan_id){
  auto* mapupdate_spec = static_cast<MapJoinSpec*>(specs_[plan_id].spec.get());
  if (mapupdate_spec->map_collection_id == mapupdate_spec->update_collection_id) {
    int max_version = versions_[plan_id] + mapupdate_spec->staleness + 1;
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
    auto& collection_view = elem_->collection_map->Get(mapupdate_spec->map_collection_id);
   
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
  auto* mapupdate_spec = static_cast<MapJoinSpec*>(specs_[ctrl.plan_id].spec.get());
  auto& collection_view = elem_->collection_map->Get(mapupdate_spec->map_collection_id);
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
  auto& part_versions = update_part_versions_[ctrl.plan_id];
  auto& node_versions = update_node_versions_[ctrl.plan_id];
  auto& node_count = update_node_count_[ctrl.plan_id];
  auto* mapupdate_spec = static_cast<MapJoinSpec*>(specs_[ctrl.plan_id].spec.get());
  auto& collection_view = elem_->collection_map->Get(mapupdate_spec->update_collection_id);
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
      LOG(INFO) << "node: " << node_id << ", update version: " << node_versions[node_id].first;
      // update node_count to next version
      node_count[node_id] = 0;
      for (auto& pv : part_versions) {
        if (part_to_node_map[pv.first] == node_id && pv.second.first == node_versions[node_id].first) {
          node_count[node_id] += 1;
        }
      }
      // try update version
      bool update_update_version = true;
      for (auto& node_version : node_versions) {
        if (node_count[node_version.first] == 0) {  // no update part there
          continue;
        }
        if (node_version.second.first == versions_[ctrl.plan_id]) {
          update_update_version = false;
          break;
        }
      }
      if (update_update_version) {
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

  auto* mapupdate_spec = static_cast<MapJoinSpec*>(specs_[plan_id].spec.get());
  if (dynamic_cast<MapWithJoinSpec*>(specs_[plan_id].spec.get()) != nullptr &&
      mapupdate_spec->map_collection_id == mapupdate_spec->update_collection_id
      ) { // only for mapwith and co-allocated
    auto* mapupdate_spec = static_cast<MapWithJoinSpec*>(specs_[plan_id].spec.get());
    if (mapupdate_spec->map_collection_id != mapupdate_spec->with_collection_id) {
      // no need to migrate with_part if map collection equals with collection
      auto& collection_view = elem_->collection_map->Get(mapupdate_spec->with_collection_id);
      auto& part_to_node = collection_view.mapper.Mutable();

      std::string url = collection_status_->GetLastCP(mapupdate_spec->with_collection_id);
      std::vector<int> with_parts;
      for (std::tuple<int, int, int> submeta : meta) {
        int from_id = std::get<0>(submeta);
        int to_id = std::get<1>(submeta);
        int part_id = std::get<2>(submeta);
        CHECK_EQ(from_id, part_to_node[part_id])
          << "from id: " << from_id << " to id: " << to_id << " part id: " << part_id << " "
          << collection_view.mapper.DebugString();
        part_to_node[part_id] = to_id;
        with_parts.push_back(std::get<2>(submeta));
      }
      int with_collection_id = mapupdate_spec->with_collection_id;
      checkpoint_loader_->LoadCheckpointPartial(mapupdate_spec->with_collection_id,
          url, with_parts, [this, plan_id, meta, with_collection_id]() {
        //load checkpoint finished
        //1. update with collection view
        LOG(INFO) << GREEN("[ControlManager::PreBatchMigrate] load checkpoint finished");
        collection_manager_->Update(with_collection_id,
            [this, plan_id, meta, with_collection_id](){
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
  auto* mapupdate_spec = static_cast<MapJoinSpec*>(specs_[plan_id].spec.get());
  auto& collection_view = elem_->collection_map->Get(mapupdate_spec->update_collection_id);
  auto& part_to_node = collection_view.mapper.Mutable();
  for (std::tuple<int, int, int> submeta : meta) {
    int from_id = std::get<0>(submeta);
    int to_id= std::get<1>(submeta);
    int part_id = std::get<2>(submeta);
    CHECK_LT(part_id, part_to_node.size());
    CHECK_EQ(part_to_node[part_id], from_id) << ", part_id: " << part_id;
    
    //migrate update 
    part_to_node[part_id] = to_id;
    auto current_time = std::chrono::system_clock::now();
    CHECK_EQ(update_part_versions_[plan_id][part_id].first, versions_[plan_id]) << "only migrate for the minimum version";
    // LOG(INFO) << DebugVersions(plan_id);
    // update from_id
    // try to update update_node_versions_ and update_node_count_
    CHECK_EQ(update_part_versions_[plan_id][part_id].first, update_node_versions_[plan_id][from_id].first);
    if (update_node_count_[plan_id][from_id] > 1) {
      update_node_count_[plan_id][from_id] -= 1;
    } else if (update_node_count_[plan_id][from_id] == 1) {
      // 1. find a new min version for from_id
      int new_min = 1000000;
      for (int i = 0; i < part_to_node.size(); ++ i) {
        if (part_to_node[i] == from_id) {
          if (update_part_versions_[plan_id][i].first < new_min) {
            new_min = update_part_versions_[plan_id][i].first;
          }
        }
      }
      // 2. calc count
      int count = 0;
      for (int i = 0; i < part_to_node.size(); ++ i) {
        if (part_to_node[i] == from_id) {
          if (update_part_versions_[plan_id][i].first == new_min) {
            count += 1;
          }
        }
      }
      update_node_count_[plan_id][from_id] = count;
      update_node_versions_[plan_id][from_id].first = new_min;
      update_node_versions_[plan_id][from_id].second = current_time;
      LOG(INFO) << "[ControlManager::Migrate] update update_node_versions_ for node: " << from_id << " to " << new_min << ", min_count: " << count;
    } else {
      CHECK(false);
    }

    // migrate map if map collection equals update collection
    if (mapupdate_spec->map_collection_id == mapupdate_spec->update_collection_id) {
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
    if (update_part_versions_[plan_id][part_id].first < update_node_versions_[plan_id][to_id].first) {
      update_node_count_[plan_id][to_id] = 1;
      update_node_versions_[plan_id][to_id].first = update_part_versions_[plan_id][part_id].first;
    } else if (update_part_versions_[plan_id][part_id].first == update_node_versions_[plan_id][to_id].first) {
      update_node_count_[plan_id][to_id] += 1;
    } else {
      // do nothing.
    }

    // for map
    if (mapupdate_spec->map_collection_id == mapupdate_spec->update_collection_id) {
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
    migrate_meta.collection_id = mapupdate_spec->update_collection_id;
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
  auto* mapupdate_spec = static_cast<MapJoinSpec*>(specs_[plan_id].spec.get());
  auto& collection_view = elem_->collection_map->Get(mapupdate_spec->map_collection_id);
  CHECK_NE(mapupdate_spec->map_collection_id, mapupdate_spec->update_collection_id);
  auto& part_to_node = collection_view.mapper.Get();
  CHECK_LT(part_id, part_to_node.size());
  CHECK_EQ(part_to_node[part_id], from_id);

  MigrateMeta migrate_meta;
  migrate_meta.flag = MigrateMeta::MigrateFlag::kStartMigrateMapOnly;
  migrate_meta.plan_id = plan_id;
  migrate_meta.collection_id = mapupdate_spec->map_collection_id;
  migrate_meta.partition_id = part_id;
  migrate_meta.from_id = from_id;
  migrate_meta.to_id = to_id;
  migrate_meta.num_nodes = elem_->nodes.size();
  SArrayBinStream bin;
  bin << migrate_meta << collection_view;
  SendToController(elem_, from_id, ControllerFlag::kMigratePartition, plan_id, bin);
}

void ControlManager::UpdateVersion(int plan_id) {
  auto* mapupdate_spec = specs_[plan_id].GetMapJoinSpec();
  versions_[plan_id] ++;

  if (!BGCP && mapupdate_spec->checkpoint_interval != 0 
          && versions_[plan_id] % mapupdate_spec->checkpoint_interval == 0) {
    int cp_iter = versions_[plan_id] / mapupdate_spec->checkpoint_interval;
    CHECK(mapupdate_spec->checkpoint_path.size());
    std::string checkpoint_path = mapupdate_spec->checkpoint_path;
    checkpoint_path = checkpoint_path + "/cp-" + std::to_string(cp_iter);
    collection_status_->AddCP(mapupdate_spec->update_collection_id, checkpoint_path);
    LOG(INFO) << RED("[ControlManager::UpdateVersion] checkpoint added for collection: "
            + std::to_string(mapupdate_spec->update_collection_id) + ", path: " + checkpoint_path);
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

void ControlManager::ReassignMap(int plan_id, int collection_id) {
  auto* mapupdate_spec = static_cast<MapJoinSpec*>(specs_[plan_id].spec.get());
  CHECK_EQ(collection_id, mapupdate_spec->map_collection_id) << "only support map now (no with)";
  // 1. identify the updated parts
  std::vector<std::tuple<int, int, int>> reassignments;  // part_id, from_id, to_id
  auto& cached_part_to_node = cached_part_to_node_[plan_id][collection_id];
  const auto& collection_view = elem_->collection_map->Get(collection_id);
  const auto& new_part_to_node = collection_view.mapper.Get();
  CHECK_EQ(cached_part_to_node.size(), new_part_to_node.size());
  for (int i = 0; i < new_part_to_node.size(); ++ i) {
    if (new_part_to_node[i] != cached_part_to_node[i]) {
      reassignments.push_back(std::make_tuple(i, cached_part_to_node[i], new_part_to_node[i]));
      cached_part_to_node[i] = new_part_to_node[i];
    }
  }
  // 2. update map_node_count_ and map_node_versions_
  // to_id, part_id, version
  std::vector<std::tuple<int,int,int>> t_p_v;
  for (auto t : reassignments) {
    int part_id = std::get<0>(t);
    int from_id = std::get<1>(t);
    int to_id = std::get<2>(t);
    // update the to_id
    if (map_part_versions_[plan_id][part_id].first != versions_[plan_id]) {
      // adjust the map_part_versions_ to min versions_
      map_part_versions_[plan_id][part_id].first = versions_[plan_id];
    }
    if (map_part_versions_[plan_id][part_id].first < map_node_versions_[plan_id][to_id].first) {
      map_node_count_[plan_id][to_id] = 1;
      map_node_versions_[plan_id][to_id].first = map_part_versions_[plan_id][part_id].first;
    } else if (map_part_versions_[plan_id][part_id].first == map_node_versions_[plan_id][to_id].first) {
      map_node_count_[plan_id][part_id] += 1;
    }
    t_p_v.push_back(std::make_tuple(to_id, part_id, map_part_versions_[plan_id][part_id].first));
    LOG(INFO) << "reassigning: <to_id, part_id, version>: " 
      << to_id << ", " << part_id << ", " << map_part_versions_[plan_id][part_id].first;
  }
  // 3. send to plan_controller and trigger RunMap
  SArrayBinStream bin;
  bin << t_p_v;
  SendToAllControllers(elem_, ControllerFlag::kReassignMap, plan_id, bin);
}

void ControlManager::Init(int plan_id) {
  start_time_ = std::chrono::system_clock::now();
  for (auto& node : elem_->nodes) {
    map_node_versions_[plan_id][node.second.node.id].first = 0;
    map_node_versions_[plan_id][node.second.node.id].second = start_time_;
    map_node_count_[plan_id][node.second.node.id] = 0;

    update_node_versions_[plan_id][node.second.node.id].first = 0;
    update_node_versions_[plan_id][node.second.node.id].second = start_time_;
    update_node_count_[plan_id][node.second.node.id] = 0;
  }

  auto* mapupdate_spec = static_cast<MapJoinSpec*>(specs_[plan_id].spec.get());
  auto& map_collection_view = elem_->collection_map->Get(mapupdate_spec->map_collection_id);
  auto& map_part_to_node_map = map_collection_view.mapper.Get();
  for (int i = 0; i < map_part_to_node_map.size(); ++ i) {
    map_node_count_[plan_id][map_part_to_node_map[i]] += 1;
    map_part_versions_[plan_id][i].first = 0;
    map_part_versions_[plan_id][i].second = start_time_;
  }

  auto& update_collection_view = elem_->collection_map->Get(mapupdate_spec->update_collection_id);
  auto& update_part_to_node_map = update_collection_view.mapper.Get();
  for (int i = 0; i < update_part_to_node_map.size(); ++ i) {
    update_node_count_[plan_id][update_part_to_node_map[i]] += 1;
    update_part_versions_[plan_id][i].first = 0;
    update_part_versions_[plan_id][i].second = start_time_;
  }
  // set the cached_part_to_node_
  cached_part_to_node_[plan_id][mapupdate_spec->map_collection_id] = map_part_to_node_map;
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

  ss << "\nupdate_part_versions_: ";
  for (auto& pv : update_part_versions_[plan_id]) {
    ss << pv.first << ": " << pv.second.first << ", ";
  }
  ss << "\nupdate_node_versions_: ";
  for (auto& pv : update_node_versions_[plan_id]) {
    ss << pv.first << ": " << pv.second.first << ", ";
  }
  ss << "\nupdate_node_count_: ";
  for (auto& pv : update_node_count_[plan_id]) {
    ss << pv.first << ": " << pv.second << ", ";
  }
  return ss.str();
}

} // namespace xyz
