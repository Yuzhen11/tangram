#include <algorithm>

#include "comm/simple_sender.hpp"
#include "core/queue_node_map.hpp"
#include "core/scheduler/scheduler.hpp"

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
  case ScheduleFlag::kUpdateCollection: {
    collection_manager_->Update(bin);
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
    FinishCheckPoint(bin);
    break;
  }
  case ScheduleFlag::kFinishLoadCheckpoint: {
    FinishLoadCheckPoint(bin);
    break;
  }
  case ScheduleFlag::kFinishWritePartition: {
    write_manager_->FinishWritePartition(bin);
    break;
  }
  // case ScheduleFlag::kJoinFinish: {
  //   FinishJoin(bin);
  //   break;
  // }
  case ScheduleFlag::kControl: {
    control_manager_->Control(bin);
    break;
  }
  case ScheduleFlag::kFinishPlan: {
    // RunNextSpec();  // TODO, if a plan finishes, run next spec
    int plan_id;
    bin >> plan_id;
    dag_runner_->Finish(plan_id);
    TryRunPlan();
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
    dag_runner_.reset(new SequentialDagRunner(program_.dag));
    LOG(INFO) << "[Scheduler] Receive program: " << program_.DebugString();
  }
  register_program_count_ += 1;
  if (register_program_count_ == elem_->nodes.size()) {
    // spawn the scheduler thread
    LOG(INFO)
        << "[Scheduler] all workers registerred, start the scheduling thread";
    // scheduler_thread_ = std::thread([this]() { Run(); });
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
  LOG(INFO) << "[Scheduler] Running: " << spec.DebugString();
  if (spec.type == SpecWrapper::Type::kDistribute) {
    LOG(INFO) << "[Scheduler] Distributing: " << spec.DebugString();
    distribute_manager_->Distribute(spec);
  } else if (spec.type == SpecWrapper::Type::kLoad) {
    LOG(INFO) << "[Scheduler] Loading: " << spec.DebugString();
    block_manager_->Load(spec);
  } else if (spec.type == SpecWrapper::Type::kMapJoin
          || spec.type == SpecWrapper::Type::kMapWithJoin) {
    // currnet_spec_ = spec;
    // int expected_num_iters = static_cast<MapJoinSpec*>(currnet_spec_.spec.get())->num_iter;
    // LOG(INFO) << "[Scheduler] TryRunPlan (" << spec_count_
    //           << "/" << program_.specs.size() << ")"
    //           << " Plan Iteration (" << cur_iters_
    //           << "/" << expected_num_iters << ") " << spec.DebugString();
    // RunMap();
    control_manager_->RunPlan(spec);
  } else if (spec.type == SpecWrapper::Type::kWrite) {
    LOG(INFO) << "[Scheduler] Writing: " << spec.DebugString();
    write_manager_->Write(spec);
  } else if (spec.type == SpecWrapper::Type::kCheckpoint) {
    LOG(INFO) << "[Scheduler] Checkpointing: " << spec.DebugString();
    Checkpoint(spec);
  } else if (spec.type == SpecWrapper::Type::kLoadCheckpoint) {
    LOG(INFO) << "[Scheduler] Loading checkpoint: " << spec.DebugString();
    LoadCheckpoint(spec);
  } else {
    CHECK(false) << spec.DebugString();
  }
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

/*
void Scheduler::RunNextSpec() {
  spec_count_ += 1;
  if (spec_count_ == program_.specs.size()) {
    Exit();
  } else {
  }
}
*/



void Scheduler::FinishCheckPoint(SArrayBinStream bin) {
  // TODO
  CHECK(false);
  // RunNextSpec();
}

void Scheduler::FinishLoadCheckPoint(SArrayBinStream bin) {
  // TODO
  CHECK(false);
  // RunNextSpec();
}

void Scheduler::Checkpoint(SpecWrapper s) {
  CHECK(s.type == SpecWrapper::Type::kCheckpoint);
  auto* checkpoint_spec = static_cast<CheckpointSpec*>(s.spec.get());
  int cid = checkpoint_spec->cid;
  std::string url = checkpoint_spec->url;
  auto& collection_view = elem_->collection_map->Get(cid);
  for (int i = 0; i < collection_view.mapper.GetNumParts(); ++ i) {
    int node_id = collection_view.mapper.Get(i);
    SArrayBinStream bin;
    std::string dest_url = url + "/part-" + std::to_string(i);
    bin << cid << i << dest_url;  // collection_id, partition_id, url
    SendTo(elem_, node_id, ScheduleFlag::kCheckpoint, bin);
  }
}

void Scheduler::LoadCheckpoint(SpecWrapper s) {
  CHECK(s.type == SpecWrapper::Type::kLoadCheckpoint);
  auto* load_checkpoint_spec = static_cast<LoadCheckpointSpec*>(s.spec.get());
  int cid = load_checkpoint_spec->cid;
  std::string url = load_checkpoint_spec->url;
  auto& collection_view = elem_->collection_map->Get(cid);
  for (int i = 0; i < collection_view.mapper.GetNumParts(); ++ i) {
    int node_id = collection_view.mapper.Get(i);
    SArrayBinStream bin;
    std::string dest_url = url + "/part-" + std::to_string(i);
    bin << cid << i << dest_url;  // collection_id, partition_id, url
    SendTo(elem_, node_id, ScheduleFlag::kLoadCheckpoint, bin);
  }
}



/*
void Scheduler::RunMap() {
  SArrayBinStream bin;
  bin << currnet_spec_;
  SendToAllWorkers(elem_, ScheduleFlag::kRunMap, bin);
}

void Scheduler::RunNextIteration() {
  SArrayBinStream bin;
  bin << currnet_spec_;
  SendToAllWorkers(elem_, ScheduleFlag::kRunMap, bin);
}

void Scheduler::FinishJoin(SArrayBinStream bin) {
  num_workers_finish_a_plan_iteration_ += 1;

  if (num_workers_finish_a_plan_iteration_ == elem_->nodes.size()) {
    num_workers_finish_a_plan_iteration_ = 0;
    cur_iters_ += 1;

    int expected_num_iters = static_cast<MapJoinSpec*>(currnet_spec_.spec.get())->num_iter;
    if (cur_iters_ == expected_num_iters) {
      cur_iters_ = 0;

      RunNextSpec();
      return;
    }
    RunNextIteration();
  }
}
*/


} // namespace xyz
