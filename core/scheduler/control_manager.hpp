#pragma once

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
  void SendToAllWorkers(ControllerFlag flag, int plan_id, SArrayBinStream bin);
  //void ToScheduler(ScheduleFlag flag, SArrayBinStream bin);
  
 private:
  std::shared_ptr<SchedulerElem> elem_;

  std::map<int, std::map<int, int>> map_versions_;
  std::map<int, std::map<int, int>> join_versions_;
  std::map<int, int> versions_;
  std::map<int, int> expected_versions_;
  std::map<int, std::set<int>> is_setup_;
  std::map<int, std::set<int>> is_finished_;
};


} // namespace xyz

