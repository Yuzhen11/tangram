#include "core/scheduler/control_manager.hpp"

#include "core/queue_node_map.hpp"

namespace xyz {

void ControlManager::Control(SArrayBinStream bin) {
  ControllerMsg ctrl;
  bin >> ctrl;
  if (ctrl.flag == ControllerMsg::Flag::kSetup) {
    is_setup_[ctrl.plan_id].insert(ctrl.node_id);
    if (is_setup_[ctrl.plan_id].size() == elem_->nodes.size()) {
      SArrayBinStream reply_bin;
      SendToAllWorkers(ControllerFlag::kStart, ctrl.plan_id, reply_bin);
    }
  } else if (ctrl.flag == ControllerMsg::Flag::kMap) {
    CHECK_EQ(map_versions_[ctrl.plan_id][ctrl.node_id] + 1, ctrl.version) << "update version 1 every time? ";
    map_versions_[ctrl.plan_id][ctrl.node_id] = ctrl.version;
    TryUpdateVersion(ctrl.plan_id);
  } else if (ctrl.flag == ControllerMsg::Flag::kJoin) {
    CHECK_EQ(join_versions_[ctrl.plan_id][ctrl.node_id] + 1, ctrl.version) << "update version 1 every time? ";
    join_versions_[ctrl.plan_id][ctrl.node_id] = ctrl.version;
    TryUpdateVersion(ctrl.plan_id);
  } else if (ctrl.flag == ControllerMsg::Flag::kFinish) {
    is_finished_[ctrl.plan_id].insert(ctrl.node_id);
    if (is_finished_[ctrl.plan_id].size() == elem_->nodes.size()) {
      SArrayBinStream reply_bin;
      ToScheduler(ScheduleFlag::kFinishPlan, reply_bin);
    }
  }
}

void ControlManager::TryUpdateVersion(int plan_id) {
  int current_version = versions_[plan_id];
  for (auto v: map_versions_[plan_id]) {
    if (current_version == v.second) {
      return;
    }
  }
  for (auto v: join_versions_[plan_id]) {
    if (current_version == v.second) {
      return;
    }
  }
  versions_[plan_id] ++;
  if (versions_[plan_id] == expected_versions_[plan_id]) {
    // send finish
    SArrayBinStream bin;
    bin << int(-1);
    SendToAllWorkers(ControllerFlag::kUpdateVersion, plan_id, bin);
  } else {
    // update version
    SArrayBinStream bin;
    bin << versions_[plan_id];
    SendToAllWorkers(ControllerFlag::kUpdateVersion, plan_id, bin);
  }
}

void ControlManager::RunPlan(SpecWrapper spec) {
  CHECK(spec.type == SpecWrapper::Type::kMapJoin
       || spec.type == SpecWrapper::Type::kMapWithJoin);

  int plan_id = spec.id;

  is_setup_[plan_id].clear();
  is_finished_[plan_id].clear();
  map_versions_[plan_id].clear();
  join_versions_[plan_id].clear();
  versions_[plan_id] = 0;
  expected_versions_[plan_id] == static_cast<MapJoinSpec*>(spec.spec.get())->num_iter;

  for (auto& node : elem_->nodes) {
    map_versions_[plan_id][node.second.node.id] = 0;
    join_versions_[plan_id][node.second.node.id] = 0;
  }
  SArrayBinStream bin;
  bin << spec;
  SendToAllWorkers(ControllerFlag::kSetup, plan_id, bin);
}

void ControlManager::SendToAllWorkers(ControllerFlag flag, int plan_id, SArrayBinStream bin) {
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

void ControlManager::ToScheduler(ScheduleFlag flag, SArrayBinStream bin) {
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
