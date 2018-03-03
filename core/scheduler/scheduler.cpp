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

    load_count_ += 1;
    TryLoad();
  }
}

void Scheduler::TryLoad() {
  if (load_count_ == program_.load_plans.size()) {
    load_done_promise_.set_value();
  } else {
    auto lp = program_.load_plans[load_count_];
    std::vector<std::pair<std::string, int>> assigned_nodes(nodes_.size());
    std::transform(
      nodes_.begin(), nodes_.end(), assigned_nodes.begin(),
      [] (Node const& node){
      return std::make_pair(node.hostname, node.id);
      });  
    CHECK(assigner_);
    int num_blocks = assigner_->Load(lp.load_collection_id, lp.url, assigned_nodes, 1);
    LOG(INFO) << "[Scheduler] Loading the " << distribute_count_ << " collection";
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
    for (int i = 0; i < part_to_node.size(); ++ i) {
      CHECK(distribute_map_[collection_id].find(i) != distribute_map_[collection_id].end());
      part_to_node[i] = distribute_map_[collection_id][i];
    }
    CollectionView cv;
    cv.collection_id = collection_id;
    cv.mapper = SimplePartToNodeMapper(part_to_node);
    cv.num_partition = cv.mapper.GetNumParts();
    collection_map_[collection_id] = cv;

    distribute_count_ += 1;
    TryDistribute();
  }
}

void Scheduler::TryDistribute() {
  if (distribute_count_ == program_.collections.size()) {
    distribute_done_promise_.set_value();
  } else {
    auto collection = program_.collections[distribute_count_];
    distribute_part_expected_ = collection.num_partition;
    // round-robin
    int node_index = 0;
    for (int i = 0; i < collection.num_partition; ++ i) {
      Message msg;
      msg.meta.sender = 0;
      msg.meta.recver = GetWorkerQid(nodes_[node_index].id);
      msg.meta.flag = Flag::kOthers;
      SArrayBinStream ctrl_bin, bin;
      ctrl_bin << ScheduleFlag::kDistribute;
      bin << i << collection;
      msg.AddData(ctrl_bin.ToSArray());
      msg.AddData(bin.ToSArray());
      sender_->Send(std::move(msg));

      node_index += 1;
      node_index %= nodes_.size();
    }
    LOG(INFO) << "[Scheduler] Distributing the " << distribute_count_ << " collection";
  }
}

void Scheduler::FinishJoin(SArrayBinStream bin) {
  LOG(INFO) << "[Scheduler] FinishJoin (" << num_workers_finish_a_plan_ + 1 << "/" << nodes_.size() << ")";
  num_workers_finish_a_plan_ += 1;
  
  if (num_workers_finish_a_plan_ == nodes_.size()) {
    num_workers_finish_a_plan_ = 0;
    program_num_plans_finished_ += 1;
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

