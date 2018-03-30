#pragma once

#include <chrono>
#include "core/scheduler/scheduler_elem.hpp"
#include "core/scheduler/control.hpp"
#include "core/scheduler/checkpoint_loader.hpp"
#include "core/scheduler/collection_status.hpp"

#include "core/plan/spec_wrapper.hpp"

namespace xyz {

class ControlManager {
 public:
  ControlManager(std::shared_ptr<SchedulerElem> elem,
          std::shared_ptr<CheckpointLoader> cp_loader,
          std::shared_ptr<CollectionStatus> collection_status)
      : elem_(elem), checkpoint_loader_(cp_loader),
        collection_status_(collection_status) {}

  void Control(SArrayBinStream bin);
  void RunPlan(SpecWrapper spec);
  //void ToScheduler(ScheduleFlag flag, SArrayBinStream bin);
  int GetCurVersion(int plan_id);
  
  void Migrate(int plan_id);
  void TrySpeculativeMap(int plan_id);
  void PrintMapVersions(int plan_id);
  using Timepoint = std::chrono::system_clock::time_point;

  void HandleUpdateMapVersion(ControllerMsg ctrl);
  void HandleUpdateJoinVersion(ControllerMsg ctrl);
  void UpdateVersion(int plan_id);
  void Init(int plan_id);
  void Migrate(int plan_id, int from_id, int to_id, int part_id);
  void MigrateMapOnly(int plan_id, int from_id, int to_id, int part_id);
  void TryMigrate(int plan_id);
 private:
  std::shared_ptr<SchedulerElem> elem_;

  std::map<int, int> versions_;
  std::map<int, int> expected_versions_;

  std::map<int, std::set<int>> is_setup_;
  std::map<int, std::set<int>> is_finished_;
  std::map<int, std::vector<Timepoint>> version_time_;

  std::map<int, SpecWrapper> specs_;
  Timepoint start_time_;

  std::map<int, std::vector<int>> parts_migrated; //part id, versions
  std::map<int, std::pair<std::chrono::system_clock::time_point, std::chrono::system_clock::time_point>> migrate_time;//migrate part id, start time, end time

  // plan_id -> part_id -> {version, time}
  std::map<int, std::map<int, std::pair<int, Timepoint>>> map_part_versions_;
  // plan_id -> node_id -> {version, time}
  std::map<int, std::map<int, std::pair<int, Timepoint>>> map_node_versions_;
  // plan_id -> node_id -> # min_version part
  std::map<int, std::map<int, int>> map_node_count_;

  // plan_id -> part_id -> {version, time}
  std::map<int, std::map<int, std::pair<int, Timepoint>>> join_part_versions_;
  // plan_id -> node_id -> {version, time}
  std::map<int, std::map<int, std::pair<int, Timepoint>>> join_node_versions_;
  // plan_id -> node_id -> # min_version part
  std::map<int, std::map<int, int>> join_node_count_;

  std::string DebugVersions(int plan_id);

  std::shared_ptr<CheckpointLoader> checkpoint_loader_;
  std::shared_ptr<CollectionStatus> collection_status_;
};


} // namespace xyz

