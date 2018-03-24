#pragma once

#include <chrono>
#include "core/scheduler/scheduler_elem.hpp"
#include "core/scheduler/control.hpp"

#include "core/plan/spec_wrapper.hpp"

namespace xyz {

class ControlManager {
 public:
  ControlManager(std::shared_ptr<SchedulerElem> elem)
      : elem_(elem) {}
  void Control(SArrayBinStream bin);
  void RunPlan(SpecWrapper spec);
  void TryUpdateVersion(int plan_id);
  void SendToAllControllers(ControllerFlag flag, int plan_id, SArrayBinStream bin);
  void SendToController(int node_id, ControllerFlag flag, int plan_id, SArrayBinStream bin);
  //void ToScheduler(ScheduleFlag flag, SArrayBinStream bin);
  int GetCurVersion(int plan_id);
  
  void Migrate(int plan_id);
  void TrySpeculativeMap(int plan_id);
  void PrintMapVersions(int plan_id);
  using Timepoint = std::chrono::system_clock::time_point;
 private:
  std::shared_ptr<SchedulerElem> elem_;

  // plan_id -> node_id -> {version, time}
  std::map<int, std::map<int, std::pair<int, Timepoint>>> map_versions_;
  std::map<int, std::map<int, int>> join_versions_;
  std::map<int, int> versions_;
  std::map<int, int> expected_versions_;
  std::map<int, std::set<int>> is_setup_;
  std::map<int, std::set<int>> is_finished_;
  std::map<int, std::vector<Timepoint>> version_time_;

  std::map<int, SpecWrapper> specs_;
  Timepoint start_time_;

  int migrate_control_ = true;  // TODO: remove this
};


} // namespace xyz

