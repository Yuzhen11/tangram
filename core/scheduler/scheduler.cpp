#include <algorithm>

#include "core/scheduler/scheduler.hpp"
#include "comm/simple_sender.hpp"
#include "core/queue_node_map.hpp"

namespace xyz {

void Scheduler::Process(Message msg) {
  CHECK_EQ(msg.data.size(), 2);  // cmd, content
  SArrayBinStream ctrl_bin, bin;
  ctrl_bin.FromSArray(msg.data[0]);
  bin.FromSArray(msg.data[1]);
  ScheduleFlag flag;
  ctrl_bin >> flag;
  switch (flag) {
    case ScheduleFlag::kRegisterProgram: {
      RegisterProgram(bin);
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
    case ScheduleFlag::kJoinFinish: {
      FinishJoin(bin);
      break;
    }
    default: CHECK(false) << ScheduleFlagName[static_cast<int>(flag)];
  }
}

void Scheduler::RegisterProgram(SArrayBinStream bin) {
  if (!init_program_) {
    init_program_ = true;
    bin >> program_;
    LOG(INFO) << "[Scheduler] Receive program: " << program_.DebugString();
  }
  register_program_count_ += 1;
  if (register_program_count_ == nodes_.size()) {
    // spawn the scheduler thread
    LOG(INFO) << "[Scheduler] all workers registerred, start the scheduling thread";
    scheduler_thread_ = std::thread([this]() { Run(); });
  }
}

void Scheduler::InitWorkers() {
  // init the partitions
  for (auto kv: collection_map_) {
    LOG(INFO) << "[Scheduler] collection: " << kv.second.DebugString();
  }

  LOG(INFO) << "[Scheduler] Initworker";
  // Send the collection_map_ to all workers.
  LOG(INFO) << "[Scheduler] size: " << collection_map_.size();
  SArrayBinStream bin;
  bin << collection_map_;
  SendToAllWorkers(ScheduleFlag::kInitWorkers, bin);
}

void Scheduler::InitWorkersReply(SArrayBinStream bin) {
  init_reply_count_ += 1;
  if (init_reply_count_ == nodes_.size()) {
    init_worker_reply_promise_.set_value();
  }
}

void Scheduler::StartScheduling() {
  // TODO
  //RunDummy();
  //Exit();
  TryRunPlan();
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

void Scheduler::RunMap(PlanSpec plan) {
  SArrayBinStream bin;
  //CHECK_EQ(program_.plans.size(), 1);
  //bin << program_.plans[0].plan_id;
  bin << plan.plan_id;
  SendToAllWorkers(ScheduleFlag::kRunMap, bin);
}

void Scheduler::PrepareNextCollection() {
  // find next collection that needs to load/distribute
  prepare_collection_count_ += 1;
  if (prepare_collection_count_ == program_.collections.size()) {
    prepare_collection_promise_.set_value();
    return;
  } else {
    auto c = program_.collections[prepare_collection_count_];
    if (c.source == CollectionSource::kLoad) {
      LOG(INFO) << "[Scheduler] Loading collection: " << c.collection_id;
      Load(c);
    } else if (c.source == CollectionSource::kDistribute || c.source == CollectionSource::kOthers) {
      LOG(INFO) << "[Scheduler] Distributing collection: " << c.collection_id;
      Distribute(c);
    } else {
      CHECK(false) << c.DebugString();
    }
  }
}

void Scheduler::FinishBlock(SArrayBinStream bin) {
  FinishedBlock block;
  bin >> block;
  LOG(INFO) << "[Scheduler] FinishBlock";
  bool done = assigner_->FinishBlock(block);
  if (done) {
    auto blocks = assigner_->GetFinishedBlocks();
    stored_blocks_[block.collection_id] = blocks;
    // construct the collection view
    std::vector<int> part_to_node(blocks.size());
    for (int i = 0; i < part_to_node.size(); ++ i) {
      CHECK(blocks.find(i) != blocks.end()) << "unknown block id " << i;
      part_to_node[i] = blocks[i].node_id;
    }
    CollectionView cv;
    cv.collection_id = block.collection_id;
    cv.mapper = SimplePartToNodeMapper(part_to_node);
    cv.num_partition = cv.mapper.GetNumParts();
    collection_map_[cv.collection_id] = cv;

    PrepareNextCollection();
  }
}

void Scheduler::Load(CollectionSpec c) {
  std::vector<std::pair<std::string, int>> assigned_nodes(nodes_.size());
  std::transform(
    nodes_.begin(), nodes_.end(), assigned_nodes.begin(),
    [] (Node const& node){
    return std::make_pair(node.hostname, node.id);
    });  
  CHECK(assigner_);
  int num_blocks = assigner_->Load(c.collection_id, c.load_url, assigned_nodes, 1);
}

void Scheduler::FinishDistribute(SArrayBinStream bin) {
  LOG(INFO) << "[Scheduler] FinishDistribute";
  int collection_id, part_id, node_id;
  bin >> collection_id >> part_id >> node_id;
  distribute_map_[collection_id][part_id] = node_id;
  if (distribute_map_[collection_id].size() == distribute_part_expected_) {
    // construct the collection view
    std::vector<int> part_to_node(distribute_part_expected_);
    for (int i = 0; i < part_to_node.size(); ++ i) {
      CHECK(distribute_map_[collection_id].find(i) != distribute_map_[collection_id].end());
      part_to_node[i] = distribute_map_[collection_id][i];
    }
    CollectionView cv;
    cv.collection_id = collection_id;
    cv.mapper = SimplePartToNodeMapper(part_to_node);
    cv.num_partition = cv.mapper.GetNumParts();
    collection_map_[collection_id] = cv;

    PrepareNextCollection();
  }
}

void Scheduler::Distribute(CollectionSpec c) {
  distribute_part_expected_ = c.num_partition;
  // round-robin
  int node_index = 0;
  for (int i = 0; i < c.num_partition; ++ i) {
    Message msg;
    msg.meta.sender = 0;
    msg.meta.recver = GetWorkerQid(nodes_[node_index].id);
    msg.meta.flag = Flag::kOthers;
    SArrayBinStream ctrl_bin, bin;
    ctrl_bin << ScheduleFlag::kDistribute;
    bin << i << c;
    msg.AddData(ctrl_bin.ToSArray());
    msg.AddData(bin.ToSArray());
    sender_->Send(std::move(msg));

    node_index += 1;
    node_index %= nodes_.size();
  }
}

void Scheduler::FinishJoin(SArrayBinStream bin) {
  //LOG(INFO) << "[Scheduler] FinishJoin:"
  // << " num_workers_finish_a_plan_iteration_ (" << num_workers_finish_a_plan_iteration_ << ", " << nodes_.size() << ")"
  //  << " num_plan_iteration_finished_ (" << num_plan_iteration_finished_ << ", " << program_.plans[program_num_plans_finished_].num_iter << ")"
  //  << " program_num_plans_finished_ (" << program_num_plans_finished_ << ", " << program_.plans.size() << ")";

  num_workers_finish_a_plan_iteration_ += 1;
  
  if (num_workers_finish_a_plan_iteration_ == nodes_.size()) {
    num_workers_finish_a_plan_iteration_ = 0;
    num_plan_iteration_finished_ += 1;

    if (num_plan_iteration_finished_ == program_.plans[program_num_plans_finished_].num_iter){
      num_plan_iteration_finished_ = 0;
      program_num_plans_finished_ += 1;
    }

    TryRunPlan();
  }
}

void Scheduler::TryRunPlan() {
  if (program_num_plans_finished_  == program_.plans.size()) {
    LOG(INFO) << "[Scheduler] Finish all plans";
    Exit();
  } else {
    LOG(INFO) << "[Scheduler] TryRunPlan (" << program_num_plans_finished_ << "/" << program_.plans.size() << ")";
    auto plan = program_.plans[program_num_plans_finished_];
    RunMap(plan);
  }
}


void Scheduler::SendToAllWorkers(ScheduleFlag flag, SArrayBinStream bin) {
  SArrayBinStream ctrl_bin;
  ctrl_bin << flag;
  for (auto node : nodes_) {
    Message msg;
    msg.meta.sender = 0;
    msg.meta.recver = GetWorkerQid(node.id);
    msg.meta.flag = Flag::kOthers;
    msg.AddData(ctrl_bin.ToSArray());
    msg.AddData(bin.ToSArray());
    sender_->Send(std::move(msg));
  }
}

}  // namespace xyz

