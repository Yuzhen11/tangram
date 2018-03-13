#include "core/scheduler/scheduler_elem.hpp"


namespace xyz {

void SendToAllWorkers(std::shared_ptr<SchedulerElem> elem, ScheduleFlag flag, SArrayBinStream bin) {
  SArrayBinStream ctrl_bin;
  ctrl_bin << flag;
  for (auto& node : elem->nodes) {
    Message msg;
    msg.meta.sender = 0;
    msg.meta.recver = GetWorkerQid(node.second.node.id);
    msg.meta.flag = Flag::kOthers;
    msg.AddData(ctrl_bin.ToSArray());
    msg.AddData(bin.ToSArray());
    elem->sender->Send(std::move(msg));
  }
}

void SendTo(std::shared_ptr<SchedulerElem> elem, int node_id, ScheduleFlag flag, SArrayBinStream bin) {
  SArrayBinStream ctrl_bin;
  ctrl_bin << flag;
  Message msg;
  msg.meta.sender = 0;
  msg.meta.recver = GetWorkerQid(node_id);
  msg.meta.flag = Flag::kOthers;
  msg.AddData(ctrl_bin.ToSArray());
  msg.AddData(bin.ToSArray());
  elem->sender->Send(std::move(msg));
}

void ToScheduler(std::shared_ptr<SchedulerElem> elem, ScheduleFlag flag, SArrayBinStream bin) {
  SArrayBinStream ctrl_bin;
  ctrl_bin << flag;
  Message msg;
  msg.meta.sender = -1;
  msg.meta.recver = 0;
  msg.meta.flag = Flag::kOthers;
  msg.AddData(ctrl_bin.ToSArray());
  msg.AddData(bin.ToSArray());
  elem->sender->Send(std::move(msg));
}

}