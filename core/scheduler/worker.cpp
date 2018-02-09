#include "core/scheduler/worker.hpp"
#include "core/plan/plan_spec.hpp"

namespace xyz {

void Worker::Process(Message msg) {
  CHECK_EQ(msg.data.size(), 2);  // cmd, content
  SArrayBinStream ctrl_bin;
  SArrayBinStream bin;
  ctrl_bin.FromSArray(msg.data[0]);
  bin.FromSArray(msg.data[1]);
  ScheduleFlag flag;
  ctrl_bin >> flag;
  switch (flag) {
    case ScheduleFlag::kInitWorkers: {
      InitWorkers(bin);
      break;
    }
    default: CHECK(false);
  }
}

void Worker::InitWorkers(SArrayBinStream bin) {
}

void Worker::RegisterPlan(PlanSpec plan) {
  SArrayBinStream ctrl_bin;
  ctrl_bin << ScheduleFlag::kRegisterPlan;
  SArrayBinStream bin;
  bin << plan;
  SendMsgToScheduler(ctrl_bin, bin);
}

void Worker::SendMsgToScheduler(SArrayBinStream ctrl_bin, SArrayBinStream bin) {
  Message msg;
  // TODO: Fill the meta
  msg.AddData(ctrl_bin.ToSArray());
  msg.AddData(bin.ToSArray());
}

}  // namespace xyz


