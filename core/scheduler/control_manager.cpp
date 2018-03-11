#include "core/scheduler/control_manager.hpp"

#include "core/scheduler/control_manager.hpp"

namespace xyz {

void ControlManager::Control(SArrayBinStream bin) {
}

void ControlManager::RunPlan(SpecWrapper spec) {
}

void ControlManager::ToController(ScheduleFlag flag, SArrayBinStream bin) {
  SArrayBinStream ctrl_bin;
  ctrl_bin << flag;
  Message msg;
  msg.meta.sender = -1;
  msg.meta.recver = 0;
  msg.meta.flag = Flag::kOthers;
  msg.AddData(ctrl_bin.ToSArray());
  msg.AddData(bin.ToSArray());
  elem_->sender->Send(std::move(msg));
}


} // namespace xyz
