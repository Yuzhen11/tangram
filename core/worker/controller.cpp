#include "core/worker/controller.hpp"

#include "core/scheduler/control.hpp"

#include "core/worker/plan_controller.hpp"

namespace xyz {

void Controller::Process(Message msg) {
  // cmd, plan_id, content
  SArrayBinStream ctrl_bin, plan_bin, bin;
  ctrl_bin.FromSArray(msg.data[0]);
  plan_bin.FromSArray(msg.data[1]);
  bin.FromSArray(msg.data[2]);
  ControllerFlag flag;
  ctrl_bin >> flag;
  int plan_id;
  plan_bin >> plan_id;

  if (flag != ControllerFlag::kSetup) {
    CHECK(plan_controllers_.find(plan_id) != plan_controllers_.end());
  }

  switch (flag) {
  case ControllerFlag::kSetup: {
    Setup(bin);
    break;
  }
  case ControllerFlag::kTerminatePlan: {
    TerminatePlan(plan_id);
    break;  
  }
  case ControllerFlag::kStart: {
    plan_controllers_[plan_id]->StartPlan();
    break;
  }
  case ControllerFlag::kFinishMap: {
    plan_controllers_[plan_id]->FinishMap(bin);
    break;
  }
  case ControllerFlag::kFinishJoin: {
    plan_controllers_[plan_id]->FinishJoin(bin);
    break;
  }
  case ControllerFlag::kUpdateVersion: {
    plan_controllers_[plan_id]->UpdateVersion(bin);
    break;
  }
  case ControllerFlag::kReceiveJoin: {
    plan_controllers_[plan_id]->ReceiveJoin(msg);
    break;
  }
  case ControllerFlag::kFetchRequest: {
    plan_controllers_[plan_id]->ReceiveFetchRequest(msg);
    break;
  }
  case ControllerFlag::kFinishFetch: {
    plan_controllers_[plan_id]->FinishFetch(bin);
    break;  
  }
  case ControllerFlag::kFinishCheckpoint: {
    plan_controllers_[plan_id]->FinishCheckpoint(bin);
    break;  
  }
  case ControllerFlag::kMigratePartition: {
    plan_controllers_[plan_id]->MigratePartition(msg);
    break;  
  }
  default: CHECK(false);
  }
}

void Controller::Setup(SArrayBinStream bin) {
  SpecWrapper spec;
  bin >> spec;

  int plan_id = spec.id;
  auto plan_controller = std::make_shared<PlanController>(this);
  plan_controllers_.insert({plan_id, plan_controller});
  plan_controllers_[plan_id]->Setup(spec);
}

// terminate a plan
// TODO: there may be some tasks still running or pending in the executors.
// For map (without fetch), join and fetch, should be ok. 
// The only possible remaining task is map (with fetch).
void Controller::TerminatePlan(int plan_id) {
  CHECK(plan_controllers_.find(plan_id) != plan_controllers_.end());
  LOG(INFO) << "[Controller] Terminating plan " << plan_id << " on node: " << engine_elem_.node.id;
  plan_controllers_.erase(plan_id);
  LOG(INFO) << "[Controller] executor size: " << engine_elem_.executor->GetNumPendingTask();
}

}  // namespace xyz

