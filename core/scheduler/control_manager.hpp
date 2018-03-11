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
  void ToController(ScheduleFlag flag, SArrayBinStream bin);
 private:
  std::shared_ptr<SchedulerElem> elem_;
};


} // namespace xyz

