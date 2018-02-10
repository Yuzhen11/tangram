#pragma once

#include <memroy>
#include <mutex>

#include "core/plan/plan_spec.hpp"
#include "core/partition/abstract_partition.hpp"
#include "core/partition/partition_manager.hpp"
#include "core/partition/tracker.hpp"

namespace xyz {

/*
 * A thread-safe structure to protect the partitions.
 */
class PartitionTracker {
 public:
  PartitionTracker() = default;

  void SetPlan(PlanSpec plan);

  // Called by map
  void RunAllMap(std::function<void(std::shared_ptr<AbstractPartition>)> func);
  void FinishMap(int part_id);

  // Called by join
  void RunJoin(int part_id, int upstream_part_id, 
          std::function<void(std::shared_ptr<AbstractPartition>)> func);
  void StartJoin(int part_id, int upstream_part_id);
  void FinishJoin(int part_id, int upstream_part_id);
 private:
  PartitionManager partitions_;
  PlanSpec plan_;

  std::mutex mu_;
  std::set<int> unassigned_map_parts_;
  std::set<int> unfinished_map_parts_;
  std::map<int, std::vector<std::function<void()>>> pending_join_;
  JoinTracker join_tracker_;

  std::shared_ptr<Executor> executor_;
};

}  // namespace

