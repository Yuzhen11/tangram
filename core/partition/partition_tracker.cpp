#include "core/partition/partition_tracker.hpp"
#include "core/queue_node_map.hpp"

namespace xyz {

void PartitionTracker::SetPlan(PlanSpec plan) {
  std::unique_lock<std::mutex> lk(mu_);
  plan_ = plan;
  // setup
  auto parts = partitions_->Get(plan_.map_collection_id);
  for (auto& part : parts) {
    unassigned_map_parts_.insert(part->part_id);
  }
  unfinished_map_parts_ = unassigned_map_parts_;
  int num_local_part = partitions_->GetNumLocalParts(plan_.join_collection_id);
  int num_upstream_part = collection_map_->GetNumParts(plan_.map_collection_id);

  // set the map/join tracker
  map_tracker_.reset(new MapTracker);
  join_tracker_.reset(new JoinTracker(num_local_part, num_upstream_part));

  plan_set_ = true;
  cond_.notify_one();
}

void PartitionTracker::RunAllMap(std::function<void(ShuffleMeta, 
                                                    std::shared_ptr<AbstractPartition>,
                                                    std::shared_ptr<AbstractMapProgressTracker>)> func) {
  std::lock_guard<std::mutex> lk(mu_);
  CHECK(plan_set_);

  // run
  auto parts = partitions_->Get(plan_.map_collection_id);
  for (auto part : parts) {
    int part_id = part->part_id;
    {
      boost::shared_lock<boost::shared_mutex> lk(part->mu);
      map_tracker_->Add(part_id, part->partition->GetSize());
    }
    executor_->Add([this, part_id, part, func](){ 
      StartMap(part_id);
      {
        boost::shared_lock<boost::shared_mutex> lk(part->mu);
        ShuffleMeta meta;
        meta.plan_id = plan_.plan_id;
        meta.collection_id = plan_.join_collection_id;
        meta.upstream_part_id = part_id;
        meta.part_id = -1;  // leave the part_id for downstream to functionstore.
        auto tracker = map_tracker_->GetTaskTracker(part_id);
        // read-only
        func(meta, part->partition, tracker); 
      }
      FinishMap(part_id, part);
    });
  }
}

void PartitionTracker::StartMap(int part_id) {
  std::lock_guard<std::mutex> lk(mu_);
  map_tracker_->Run(part_id);
}

void PartitionTracker::FinishMap(int part_id, std::shared_ptr<VersionedPartition> part) {
  std::lock_guard<std::mutex> lk(mu_);
  unfinished_map_parts_.erase(part_id);
  if (unfinished_map_parts_.empty()) {
    // send a message to worker
    Message msg;
    msg.meta.sender = GetJoinActorQid(node_id_);
    msg.meta.recver = GetWorkerQid(node_id_);
    msg.meta.flag = Flag::kOthers;
    SArrayBinStream ctrl_bin, bin;
    msg.AddData(ctrl_bin.ToSArray());
    msg.AddData(bin.ToSArray());
    sender_->Send(msg);
  }
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
  std::unique_lock<std::mutex> lk(mu_);
  cond_.wait(lk, [this]() {
    return plan_set_ == true;
  });
  
  int part_id = join_meta.part_id; 
  int upstream_part_id = join_meta.upstream_part_id;
  auto func = join_meta.func;
  if (join_tracker_->Has(part_id, upstream_part_id)) {
    // this join has been invoked.
    return;
  } else {
    join_tracker_->Add(part_id, upstream_part_id);
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
  join_tracker_->Run(part_id, upstream_part_id);
}

void PartitionTracker::FinishJoin(int part_id, int upstream_part_id) {
  std::lock_guard<std::mutex> lk(mu_);
  join_tracker_->Finish(part_id, upstream_part_id);
  if (join_tracker_->FinishAll()) {
    // send a message to worker
    Message msg;
    msg.meta.sender = GetJoinActorQid(node_id_);
    msg.meta.recver = GetWorkerQid(node_id_);
    msg.meta.flag = Flag::kOthers;
    SArrayBinStream ctrl_bin, bin;
    msg.AddData(ctrl_bin.ToSArray());
    msg.AddData(bin.ToSArray());
    sender_->Send(msg);
  }
}

}  // namespace xyz

