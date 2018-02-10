#include "core/partition/partition_tracker.hpp"

namespace xyz {

void PartitionTracker::SetPlan(PlanSpec plan) {
  plan_ = plan;
}

void PartitionTracker::RunAllMap() {
  std::lock_guard<std::mutex> lk(mu_);
  // setup
  auto& parts = partitions_->Get(plan.map_collection_id);
  for (auto& kv : parts) {
    unassigned_map_parts_.insert(kv.first);
  }
  unfinished_map_parts_ = unassigned_map_parts_;

  // run
  for (auto part : parts) {
    int part_id = part.first;
    executor_->Add([this, part_id, part, func](){ 
      func(part.second->partition); 
      FinishMap(part_id);
    });
  }
}

void PartitionTracker::FinishMap(int part_id) {
  std::lock_guard<std::mutex> lk(mu_);
  unfinished_map_parts_.erase(part_id);
  if (pending_join_.find(part_id) != pending_join_.end()) {
    auto pending_work = pending_join_[part_id];
    for (auto work : pending_work) {
      executor_->Add();  // TODO
    }
  }
}

void PartitionTracker::RunJoin(int part_id, int upstream_part_id, 
        std::function<void(std::shared_ptr<AbstractPartition>)> func) {
  std::lock_guard<std::mutex> lk(mu_);
  if (join_tracker_.Has(part_id, upstream_part_id)) {
    // this join has been invoked.
    return;
  } else {
    join_tracker_.Add(part_id, upstream_part_id);
  }

  // the map and join are working on the same collection.
  if (plan_.join_collection_id == plan_.map_collection_id 
      && unfinished_map_parts_.find(part_id) != unfinished_map_parts_.end()) {  
    // if the part does not finish the map phase
    // we should wait until the map on this partition finishes.
    pending_join_[part_id].push_back(func);
  } else {
    auto part = partitions_->Get(plan_.join_collection_id, part_id);
    executor_->Add([this, part_id, upstream_part_id, func, part]() {
      StartJoin(part_id, upstream_part_id);
      func(part);
      FinishJoin(part_id, upstream_part_id);
    });
  }
}

void PartitionTracker::StartJoin(int part_id, int upstream_part_id) {
  std::lock_guard<std::mutex> lk(mu_);
  join_tracker_.Start(part_id, upstream_part_id);
}

void PartitionTracker::FinishJoin(int part_id, int upstream_part_id) {
  std::lock_guard<std::mutex> lk(mu_);
  join_tracker_.Finish(part_id, upstream_part_id);
}

}  // namespace

