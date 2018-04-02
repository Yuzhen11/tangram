#include <algorithm>

#include "comm/simple_sender.hpp"
#include "core/queue_node_map.hpp"
#include "core/scheduler/scheduler.hpp"

#include "base/color.hpp"

namespace xyz {

// make the scheduler ready and start receiving RegisterProgram
void Scheduler::Ready(std::vector<Node> nodes) {
  start = std::chrono::system_clock::now();
  LOG(INFO) << "[Scheduler] Ready";
  for (auto& node : nodes) {
    CHECK(elem_->nodes.find(node.id) == elem_->nodes.end());
    NodeInfo n;
    n.node = node;
    elem_->nodes[node.id] = n;
  }
  Start();
  start_ = true;
}

void Scheduler::Process(Message msg) {
  CHECK_EQ(msg.data.size(), 2); // cmd, content
  int node_id = GetNodeId(msg.meta.sender);
  SArrayBinStream ctrl_bin, bin;
  ctrl_bin.FromSArray(msg.data[0]);
  bin.FromSArray(msg.data[1]);
  ScheduleFlag flag;
  ctrl_bin >> flag;
  switch (flag) {
  case ScheduleFlag::kRegisterProgram: {
    RegisterProgram(node_id, bin);
    break;
  }
  case ScheduleFlag::kUpdateCollectionReply: {
    collection_manager_->FinishUpdate(bin);
    break;
  }
  case ScheduleFlag::kFinishBlock: {
    block_manager_->FinishBlock(bin);
    break;
  }
  case ScheduleFlag::kFinishDistribute: {
    distribute_manager_->FinishDistribute(bin);
    break;
  }
  case ScheduleFlag::kFinishCheckpoint: {
    checkpoint_manager_->FinishCheckpoint(bin);
    break;
  }
  case ScheduleFlag::kFinishLoadCheckpoint: {
    checkpoint_loader_->FinishLoadCheckpoint(bin);
    break;
  }
  case ScheduleFlag::kFinishWritePartition: {
    write_manager_->FinishWritePartition(bin);
    break;
  }
  case ScheduleFlag::kControl: {
    control_manager_->Control(bin);
    break;
  }
  case ScheduleFlag::kFinishPlan: {
    int plan_id;
    bin >> plan_id;
    LOG(INFO) << "[Scheduler] " << YELLOW("Finish plan " + std::to_string(plan_id));
    dag_runner_->Finish(plan_id);
    collection_status_->FinishPlan(plan_id);
    // LOG(INFO) << collection_status_->DebugString();
    TryRunPlan();
    break;
  }
  case ScheduleFlag::kRecovery: {
    Recovery(bin);
    break;
  }
  case ScheduleFlag::kFinishRecovery: {
    FinishRecovery();
    break;
  }
  default:
    CHECK(false) << ScheduleFlagName[static_cast<int>(flag)];
  }
}

void Scheduler::RegisterProgram(int node_id, SArrayBinStream bin) {
  WorkerInfo info;
  bin >> info;
  CHECK(elem_->nodes.find(node_id) != elem_->nodes.end());
  elem_->nodes[node_id].num_local_threads = info.num_local_threads;
  if (!init_program_) {
    init_program_ = true;
    bin >> program_;
    LOG(INFO) << "set dag_runner: " << dag_runner_type_;
    if (dag_runner_type_ == "sequential") {
      dag_runner_.reset(new SequentialDagRunner(program_.dag));
    } else if (dag_runner_type_ == "wide") {
      dag_runner_.reset(new WideDagRunner(program_.dag));
    } else {
      CHECK(false);
    }
    LOG(INFO) << "[Scheduler] Receive program: " << program_.DebugString();
  }
  register_program_count_ += 1;
  if (register_program_count_ == elem_->nodes.size()) {
    LOG(INFO)
        << "[Scheduler] all workers registerred, start the scheduling thread";
    TryRunPlan();
  }
}

void Scheduler::TryRunPlan() {
  if (dag_runner_->GetNumRemainingPlans() == 0) {
    Exit();
  } else {
    auto plans = dag_runner_->GetRunnablePlans();
    for (auto plan_id : plans) {
      RunPlan(plan_id);
    }
  }
}

void Scheduler::RunPlan(int plan_id) {
  CHECK_LT(plan_id, program_.specs.size());
  auto spec = program_.specs[plan_id];
  LOG(INFO) << "[Scheduler] " << YELLOW("Running plan "+std::to_string(spec.id)+" "+spec.name+" ") << spec.DebugString();
  auto rw = spec.GetReadWrite();
  collection_status_->AddPlan(plan_id, rw);
  if (spec.type == SpecWrapper::Type::kDistribute) {
    LOG(INFO) << "[Scheduler] Distributing: " << spec.DebugString();
    distribute_manager_->Distribute(spec);
  } else if (spec.type == SpecWrapper::Type::kLoad) {
    LOG(INFO) << "[Scheduler] Loading: " << spec.DebugString();
    block_manager_->Load(spec);
  } else if (spec.type == SpecWrapper::Type::kMapJoin
          || spec.type == SpecWrapper::Type::kMapWithJoin) {
    control_manager_->RunPlan(spec);
  } else if (spec.type == SpecWrapper::Type::kWrite) {
    LOG(INFO) << "[Scheduler] Writing: " << spec.DebugString();
    write_manager_->Write(spec);
  } else if (spec.type == SpecWrapper::Type::kCheckpoint) {
    LOG(INFO) << "[Scheduler] Checkpointing: " << spec.DebugString();
    checkpoint_manager_->Checkpoint(spec);
  } else if (spec.type == SpecWrapper::Type::kLoadCheckpoint) {
    LOG(INFO) << "[Scheduler] Loading checkpoint: " << spec.DebugString();
    checkpoint_manager_->LoadCheckpoint(spec);
  } else {
    CHECK(false) << spec.DebugString();
  }
}

void Scheduler::FinishRecovery() {
  LOG(INFO) << "[Scheduler] FinishRecovery";
  auto cur_plans = collection_status_->GetCurrentPlans();
  for (auto pid: cur_plans) {
    collection_status_->FinishPlan(pid);
    CHECK_LT(pid, program_.specs.size());
    auto& spec = program_.specs[pid];
    // TODO: now I assert it must be mj or mwj
    CHECK(spec.type == SpecWrapper::Type::kMapJoin
       || spec.type == SpecWrapper::Type::kMapWithJoin);

    if (spec.type == SpecWrapper::Type::kMapJoin
    || spec.type == SpecWrapper::Type::kMapWithJoin) {
      auto* mapjoin_spec = program_.specs[pid].GetMapJoinSpec();
      int cur_version = control_manager_->GetCurVersion(pid);
      int new_iter = mapjoin_spec->num_iter - 
          (cur_version / mapjoin_spec->checkpoint_interval * mapjoin_spec->checkpoint_interval);
      // directly update the version
      mapjoin_spec->num_iter = new_iter;
    }
    // relaunch the plan
    RunPlan(pid);
  }
}

void Scheduler::Recovery(SArrayBinStream bin) {
  // remove dead_nodes
  std::set<int> dead_nodes;
  bin >> dead_nodes;
  for (auto node : dead_nodes)
    elem_->nodes.erase(node);

  std::vector<int> remaining_nodes;
  for( auto it = elem_->nodes.begin(); it != elem_->nodes.end(); ++it) {
    remaining_nodes.push_back(it->first);
  }

  // terminate plan
  auto cur_plans = collection_status_->GetCurrentPlans();
  for (auto pid: cur_plans) {
    auto spec_wrapper = program_.specs[pid];
    CHECK(spec_wrapper.type == SpecWrapper::Type::kMapJoin
         || spec_wrapper.type == SpecWrapper::Type::kMapWithJoin);
    SArrayBinStream dummy_bin;
    SendToAllControllers(elem_, ControllerFlag::kTerminatePlan, pid, dummy_bin);
  }

  // recover the collections and update collection map
  recover_manager_->Recover(dead_nodes);
}

void Scheduler::Exit() {
  end = std::chrono::system_clock::now();
  std::chrono::duration<double> duration = end - start;
  LOG(INFO) << "[Scheduler] Exit. Runtime: " << duration.count();
  SArrayBinStream dummy_bin;
  SendToAllWorkers(elem_, ScheduleFlag::kExit, dummy_bin);
  exit_promise_.set_value();
}

void Scheduler::Wait() {
  LOG(INFO) << "[Scheduler] waiting";
  std::future<void> f = exit_promise_.get_future();
  f.get();
}

void Scheduler::RunDummy() {
  SArrayBinStream bin;
  SendToAllWorkers(elem_, ScheduleFlag::kDummy, bin);
}

} // namespace xyz
