#include "core/partition/partition_tracker.hpp"

namespace xyz {

void PartitionTracker::SetPlan(PlanSpec plan) {
  plan_ = plan;
}

void PartitionTracker::RunAllMap(std::function<void(std::shared_ptr<AbstractPartition>)> func) {
  std::lock_guard<std::mutex> lk(mu_);
  // setup
  auto parts = partitions_->Get(plan_.map_collection_id);
  for (auto& part : parts) {
    unassigned_map_parts_.insert(part->part_id);
  }
  unfinished_map_parts_ = unassigned_map_parts_;

  // run
  for (auto part : parts) {
    int part_id = part->part_id;
    executor_->Add([this, part_id, part, func](){ 
      {
        boost::shared_lock<boost::shared_mutex> lk(part->mu);
        // read-only
        func(part->partition); 
      }
      FinishMap(part_id, part);
    });
  }
}

void PartitionTracker::StartMap(int part_id, std::shared_ptr<VersionedPartition> part) {
}

void PartitionTracker::FinishMap(int part_id, std::shared_ptr<VersionedPartition> part) {
  std::lock_guard<std::mutex> lk(mu_);
  unfinished_map_parts_.erase(part_id);
  if (pending_join_.find(part_id) != pending_join_.end()) {
    auto pending_work = pending_join_[part_id];
    for (auto work : pending_work) {
      executor_->Add([this, work, part]() {
        StartJoin(work.part_id, work.upstream_part_id);
        {
          boost::unique_lock<boost::shared_mutex> lk(part->mu);  
          // have write-access
          work.func(part->partition);
        }
        FinishJoin(work.part_id, work.upstream_part_id);
      });
    }
  }
}

void PartitionTracker::RunJoin(JoinMeta join_meta) {
  std::lock_guard<std::mutex> lk(mu_);
  int part_id = join_meta.part_id; 
  int upstream_part_id = join_meta.upstream_part_id;
  auto func = join_meta.func;
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
    pending_join_[part_id].push_back(join_meta);
  } else {
    auto part = partitions_->Get(plan_.join_collection_id, part_id);
    executor_->Add([this, part_id, upstream_part_id, func, part]() {
      StartJoin(part_id, upstream_part_id);
      {
        boost::unique_lock<boost::shared_mutex> lk(part->mu);  
        // have write-access
        func(part->partition);
      }
      FinishJoin(part_id, upstream_part_id);
    });
  }
}

void PartitionTracker::StartJoin(int part_id, int upstream_part_id) {
  std::lock_guard<std::mutex> lk(mu_);
  join_tracker_.Run(part_id, upstream_part_id);
}

void PartitionTracker::FinishJoin(int part_id, int upstream_part_id) {
  std::lock_guard<std::mutex> lk(mu_);
  join_tracker_.Finish(part_id, upstream_part_id);
}

void PartitionTracker::WaitAllJoin() {
  std::unique_lock<std::mutex> lk(mu_);
  cond_.wait(lk, [this]() {
    if (join_tracker_.tracker.size() < partitions_->GetNumLocalParts(plan_.join_collection_id)) {
      return false;
    }
    for (auto kv: join_tracker_.tracker) {
      if (kv.second.finished.size() < plan_.join_partition_num)
        return false;
    }
    return true;
  });
}

}  // namespace xyz

