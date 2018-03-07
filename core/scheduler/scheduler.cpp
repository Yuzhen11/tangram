#include <algorithm>

#include "comm/simple_sender.hpp"
#include "core/queue_node_map.hpp"
#include "core/scheduler/scheduler.hpp"

namespace xyz {

// make the scheduler ready and start receiving RegisterProgram
void Scheduler::Ready(std::vector<Node> nodes) {
  LOG(INFO) << "[Scheduler] Ready";
  for (auto& node : nodes) {
    CHECK(nodes_.find(node.id) == nodes_.end());
    NodeInfo n;
    n.node = node;
    nodes_[node.id] = n;
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
  case ScheduleFlag::kInitWorkersReply: {
    InitWorkersReply(bin);
    break;
  }
  case ScheduleFlag::kFinishBlock: {
    FinishBlock(bin);
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
  CHECK(nodes_.find(node_id) != nodes_.end());
  nodes_[node_id].num_local_threads = info.num_local_threads;
  if (!init_program_) {
    init_program_ = true;
    bin >> program_;
    LOG(INFO) << "[Scheduler] Receive program: " << program_.DebugString();
  }
  register_program_count_ += 1;
  if (register_program_count_ == nodes_.size()) {
    // spawn the scheduler thread
    LOG(INFO)
        << "[Scheduler] all workers registerred, start the scheduling thread";
    scheduler_thread_ = std::thread([this]() { Run(); });
  }
}

void Scheduler::InitWorkers() {
  // init the partitions
  init_reply_count_ = 0;
  for (auto kv : collection_map_) {
    LOG(INFO) << "[Scheduler] collection: " << kv.second.DebugString();
  }

  LOG(INFO) << "[Scheduler] Initworker";
  // Send the collection_map_ to all workers.
  SArrayBinStream bin;
  bin << collection_map_;
  SendToAllWorkers(ScheduleFlag::kInitWorkers, bin);
}

void Scheduler::InitWorkersReply(SArrayBinStream bin) {
  init_reply_count_ += 1;
  if (init_reply_count_ == nodes_.size()) {
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
  LOG(INFO) << "[Scheduler] Exit";
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
    if (spec.type == SpecWrapper::Type::kDistribute) {
      LOG(INFO) << "[Scheduler] Distributing: " << spec.DebugString();
      Distribute(static_cast<DistributeSpec*>(spec.spec.get()));
    } else if (spec.type == SpecWrapper::Type::kLoad) {
      LOG(INFO) << "[Scheduler] Loading: " << spec.DebugString();
      Load(static_cast<LoadSpec*>(spec.spec.get()));
    } else if (spec.type == SpecWrapper::Type::kMapJoin) {
      currnet_spec_ = program_.specs[spec_count_];
      int expected_num_iters = static_cast<MapJoinSpec*>(currnet_spec_.spec.get())->num_iter;
      LOG(INFO) << "[Scheduler] TryRunPlan (" << spec_count_
                << "/" << program_.specs.size() << ")"
                << " Plan Iteration (" << cur_iters_
                << "/" << expected_num_iters << ")";
      RunMap();
  
    } else {
      CHECK(false) << spec.DebugString();
    }
  }
}

void Scheduler::FinishBlock(SArrayBinStream bin) {
  FinishedBlock block;
  bin >> block;
  LOG(INFO) << "[Scheduler] FinishBlock: " << block.DebugString();
  bool done = assigner_->FinishBlock(block);
  if (done) {
    auto blocks = assigner_->GetFinishedBlocks();
    stored_blocks_[block.collection_id] = blocks;
    // construct the collection view
    std::vector<int> part_to_node(blocks.size());
    for (int i = 0; i < part_to_node.size(); ++i) {
      CHECK(blocks.find(i) != blocks.end()) << "unknown block id " << i;
      part_to_node[i] = blocks[i].node_id;
    }
    CollectionView cv;
    cv.collection_id = block.collection_id;
    cv.mapper = SimplePartToNodeMapper(part_to_node);
    cv.num_partition = cv.mapper.GetNumParts();
    collection_map_[cv.collection_id] = cv;

    // PrepareNextCollection();
    // RunNextSpec();
    InitWorkers();
  }
}

void Scheduler::Load(LoadSpec* spec) {
  std::vector<std::pair<std::string, int>> assigned_nodes;
  std::vector<int> num_local_threads;
  for (auto& kv: nodes_) {
    assigned_nodes.push_back({kv.second.node.hostname, kv.second.node.id});
    num_local_threads.push_back(kv.second.num_local_threads);
  }
  CHECK(assigner_);
  int num_blocks =
      assigner_->Load(spec->collection_id, spec->url, assigned_nodes, num_local_threads);
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
    collection_map_[collection_id] = cv;

    // PrepareNextCollection();
    InitWorkers();
    // RunNextSpec();
  }
}

void Scheduler::Distribute(DistributeSpec* spec) {
  distribute_part_expected_ = spec->num_partition;
  // round-robin
  auto node_iter = nodes_.begin();
  for (int i = 0; i < spec->num_partition; ++i) {
    CHECK(node_iter != nodes_.end());
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
    sender_->Send(std::move(msg));

    node_iter ++;
    if (node_iter == nodes_.end()) {
      node_iter = nodes_.begin();
    }
  }
}

void Scheduler::FinishCheckPoint(SArrayBinStream bin) {
  // TODO
  CHECK(false);
}

void Scheduler::FinishWritePartition(SArrayBinStream bin) {
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

void Scheduler::WritePartition() {
  // TODO
  CHECK(false);
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

  if (num_workers_finish_a_plan_iteration_ == nodes_.size()) {
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
  for (auto& node : nodes_) {
    Message msg;
    msg.meta.sender = 0;
    msg.meta.recver = GetWorkerQid(node.second.node.id);
    msg.meta.flag = Flag::kOthers;
    msg.AddData(ctrl_bin.ToSArray());
    msg.AddData(bin.ToSArray());
    sender_->Send(std::move(msg));
  }
}

} // namespace xyz
