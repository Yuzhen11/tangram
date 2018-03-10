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
  case ScheduleFlag::kInitWorkers: {
    InitWorkers();
    break;
  }
  case ScheduleFlag::kInitWorkersReply: {
    InitWorkersReply(bin);
    break;
  }
  case ScheduleFlag::kFinishBlock: {
    block_manager_->FinishBlock(bin);
    break;
  }
  case ScheduleFlag::kFinishDistribute: {
    FinishDistribute(bin);
    break;
  }
  case ScheduleFlag::kFinishCheckPoint: {
    FinishCheckPoint(bin);
    break;
  }
  case ScheduleFlag::kFinishWritePartition: {
    FinishWritePartition(bin);
    break;
  }
  case ScheduleFlag::kJoinFinish: {
    FinishJoin(bin);
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
    LOG(INFO) << "[Scheduler] Receive program: " << program_.DebugString();
  }
  register_program_count_ += 1;
  if (register_program_count_ == elem_->nodes.size()) {
    // spawn the scheduler thread
    LOG(INFO)
        << "[Scheduler] all workers registerred, start the scheduling thread";
    scheduler_thread_ = std::thread([this]() { Run(); });
  }
}

void Scheduler::InitWorkers() {
  // init the partitions
  init_reply_count_ = 0;
  LOG(INFO) << "[Scheduler] CollectionMap: " << elem_->collection_map->DebugString();

  LOG(INFO) << "[Scheduler] Initworker";
  // Send the collection_map_ to all workers.
  SArrayBinStream bin;
  bin << *elem_->collection_map;
  SendToAllWorkers(ScheduleFlag::kInitWorkers, bin);
}

void Scheduler::InitWorkersReply(SArrayBinStream bin) {
  init_reply_count_ += 1;
  if (init_reply_count_ == elem_->nodes.size()) {
    // init_worker_reply_promise_.set_value();
    LOG(INFO) << "[Scheduler] Initworker Done, RunNextSpec";
    RunNextSpec();
  }
}

void Scheduler::StartScheduling() {
  // TODO
  // RunDummy();
  // Exit();
  // TryRunPlan();
}

void Scheduler::Exit() {
  end = std::chrono::system_clock::now();
  std::chrono::duration<double> duration = end - start;
  LOG(INFO) << "[Scheduler] Exit. Runtime: " << duration.count();
  SArrayBinStream dummy_bin;
  SendToAllWorkers(ScheduleFlag::kExit, dummy_bin);
  exit_promise_.set_value();
}

void Scheduler::Wait() {
  LOG(INFO) << "[Scheduler] waiting";
  std::future<void> f = exit_promise_.get_future();
  f.get();
}

void Scheduler::RunDummy() {
  SArrayBinStream bin;
  SendToAllWorkers(ScheduleFlag::kDummy, bin);
}

void Scheduler::RunNextSpec() {
  spec_count_ += 1;
  if (spec_count_ == program_.specs.size()) {
    Exit();
  } else {
    auto spec = program_.specs[spec_count_];
    LOG(INFO) << "[Scheduler] Running: " << spec.DebugString();
    if (spec.type == SpecWrapper::Type::kDistribute) {
      LOG(INFO) << "[Scheduler] Distributing: " << spec.DebugString();
      Distribute(static_cast<DistributeSpec*>(spec.spec.get()));
    } else if (spec.type == SpecWrapper::Type::kLoad) {
      LOG(INFO) << "[Scheduler] Loading: " << spec.DebugString();
      block_manager_->Load(static_cast<LoadSpec*>(spec.spec.get()));
    } else if (spec.type == SpecWrapper::Type::kMapJoin
            || spec.type == SpecWrapper::Type::kMapWithJoin) {
      currnet_spec_ = spec;
      int expected_num_iters = static_cast<MapJoinSpec*>(currnet_spec_.spec.get())->num_iter;
      LOG(INFO) << "[Scheduler] TryRunPlan (" << spec_count_
                << "/" << program_.specs.size() << ")"
                << " Plan Iteration (" << cur_iters_
                << "/" << expected_num_iters << ") " << spec.DebugString();
      RunMap();
    } else if (spec.type == SpecWrapper::Type::kWrite) {
      LOG(INFO) << "[Scheduler] Writing: " << spec.DebugString();
      Write(spec);
    // } else if (spec.type == SpecWrapper::Type::kMapWithJoin) {
    //   LOG(INFO) << "[Scheduler] spec not implemented: " << spec.DebugString();
    //   RunNextSpec();
    } else {
      CHECK(false) << spec.DebugString();
    }
  }
}


void Scheduler::FinishDistribute(SArrayBinStream bin) {
  LOG(INFO) << "[Scheduler] FinishDistribute";
  int collection_id, part_id, node_id;
  bin >> collection_id >> part_id >> node_id;
  distribute_map_[collection_id][part_id] = node_id;
  if (distribute_map_[collection_id].size() == distribute_part_expected_) {
    // construct the collection view
    std::vector<int> part_to_node(distribute_part_expected_);
    for (int i = 0; i < part_to_node.size(); ++i) {
      CHECK(distribute_map_[collection_id].find(i) !=
            distribute_map_[collection_id].end());
      part_to_node[i] = distribute_map_[collection_id][i];
    }
    CollectionView cv;
    cv.collection_id = collection_id;
    cv.mapper = SimplePartToNodeMapper(part_to_node);
    cv.num_partition = cv.mapper.GetNumParts();
    elem_->collection_map->Insert(cv);

    // PrepareNextCollection();
    InitWorkers();
    // RunNextSpec();
  }
}

void Scheduler::Distribute(DistributeSpec* spec) {
  distribute_part_expected_ = spec->num_partition;
  // round-robin
  auto node_iter = elem_->nodes.begin();
  for (int i = 0; i < spec->num_partition; ++i) {
    CHECK(node_iter != elem_->nodes.end());
    Message msg;
    msg.meta.sender = 0;
    msg.meta.recver = GetWorkerQid(node_iter->second.node.id);
    msg.meta.flag = Flag::kOthers;
    SArrayBinStream ctrl_bin, bin;
    ctrl_bin << ScheduleFlag::kDistribute;
    bin << i;
    spec->ToBin(bin);
    msg.AddData(ctrl_bin.ToSArray());
    msg.AddData(bin.ToSArray());
    elem_->sender->Send(std::move(msg));

    node_iter ++;
    if (node_iter == elem_->nodes.end()) {
      node_iter = elem_->nodes.begin();
    }
  }
}

void Scheduler::FinishCheckPoint(SArrayBinStream bin) {
  // TODO
  CHECK(false);
}

void Scheduler::CheckPoint() {
  // TODO: Check point proper collection, partition to proper dest_rul
  const int collection_id = 1;
  const int part_id = 1;
  const std::string dest_url = "/tmp/tmp/c.txt";

  SArrayBinStream bin;
  bin << collection_id << part_id << dest_url;
  SendToAllWorkers(ScheduleFlag::kCheckPoint, bin);
}

void Scheduler::Write(SpecWrapper s) {
  CHECK(s.type == SpecWrapper::Type::kWrite);
  auto* write_spec = static_cast<WriteSpec*>(s.spec.get());
  int id = write_spec->collection_id;
  std::string url = write_spec->url;
  auto& collection_view = elem_->collection_map->Get(id);
  write_reply_count_ = 0;
  expected_write_reply_count_ = collection_view.mapper.GetNumParts();
  LOG(INFO) << "[Scheduler] writing to " << expected_write_reply_count_ << " partitions";
  for (int i = 0; i < collection_view.mapper.GetNumParts(); ++ i) {
    int node_id = collection_view.mapper.Get(i);
    SArrayBinStream bin;
    std::string dest_url = url + "/part-" + std::to_string(i);
    bin << id << i << dest_url;  // collection_id, partition_id, url
    SendTo(node_id, ScheduleFlag::kWritePartition, bin);
  }
}

void Scheduler::FinishWritePartition(SArrayBinStream bin) {
  write_reply_count_ += 1;
  if (write_reply_count_ == expected_write_reply_count_) {
    RunNextSpec();
  }
}


void Scheduler::RunMap() {
  SArrayBinStream bin;
  bin << currnet_spec_;
  SendToAllWorkers(ScheduleFlag::kRunMap, bin);
}

void Scheduler::RunNextIteration() {
  SArrayBinStream bin;
  bin << currnet_spec_;
  SendToAllWorkers(ScheduleFlag::kRunMap, bin);
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

void Scheduler::SendToAllWorkers(ScheduleFlag flag, SArrayBinStream bin) {
  SArrayBinStream ctrl_bin;
  ctrl_bin << flag;
  for (auto& node : elem_->nodes) {
    Message msg;
    msg.meta.sender = 0;
    msg.meta.recver = GetWorkerQid(node.second.node.id);
    msg.meta.flag = Flag::kOthers;
    msg.AddData(ctrl_bin.ToSArray());
    msg.AddData(bin.ToSArray());
    elem_->sender->Send(std::move(msg));
  }
}

void Scheduler::SendTo(int node_id, ScheduleFlag flag, SArrayBinStream bin) {
  SArrayBinStream ctrl_bin;
  ctrl_bin << flag;
  Message msg;
  msg.meta.sender = 0;
  msg.meta.recver = GetWorkerQid(node_id);
  msg.meta.flag = Flag::kOthers;
  msg.AddData(ctrl_bin.ToSArray());
  msg.AddData(bin.ToSArray());
  elem_->sender->Send(std::move(msg));
}

} // namespace xyz
