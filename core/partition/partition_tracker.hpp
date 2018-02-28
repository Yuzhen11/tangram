#pragma once

#include <memory>
#include <mutex>
#include <condition_variable>

#include "core/plan/plan_spec.hpp"
#include "core/partition/abstract_partition.hpp"
#include "core/partition/partition_manager.hpp"
#include "core/partition/tracker.hpp"
#include "core/executor/executor.hpp"

namespace xyz {

/*
 * A thread-safe structure to protect the partitions.
 */
  
struct JoinMeta {
  int collection_id;
  int part_id;
  int upstream_part_id;
  std::function<void(std::shared_ptr<AbstractPartition>)> func;
};

class PartitionTracker {
 public:
  PartitionTracker(std::shared_ptr<PartitionManager> partition_manager,
                   std::shared_ptr<Executor> executor)
      : partitions_(partition_manager), executor_(executor) {}

  void SetPlan(PlanSpec plan);
  PlanSpec GetPlan() { return plan_; }

  // Called by map
  void RunAllMap(std::function<void(std::shared_ptr<AbstractPartition>, 
                                    std::shared_ptr<AbstractMapProgressTracker>)> func);

  // Called by join
  void RunJoin(JoinMeta join_meta);
  void WaitAllJoin();

  MapTracker* GetMapTracker() {
    return &map_tracker_;
  }
  JoinTracker* GetJoinTracker() {
    return &join_tracker_;
  }
 private:
  void StartMap(int part_id);
  void FinishMap(int part_id, std::shared_ptr<VersionedPartition> part);
  void StartJoin(int part_id, int upstream_part_id);
  void FinishJoin(int part_id, int upstream_part_id);
 private:
  std::shared_ptr<PartitionManager> partitions_;
  PlanSpec plan_;

  std::condition_variable cond_;
  std::mutex mu_;
  std::set<int> unassigned_map_parts_;
  std::set<int> unfinished_map_parts_;
  std::map<int, std::vector<JoinMeta>> pending_join_;
  JoinTracker join_tracker_;
  MapTracker map_tracker_;

  std::shared_ptr<Executor> executor_;
};

}  // namespace

