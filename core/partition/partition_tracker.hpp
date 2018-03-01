#pragma once

#include <memory>
#include <mutex>
#include <condition_variable>

#include "core/plan/plan_spec.hpp"
#include "core/partition/abstract_partition.hpp"
#include "core/partition/partition_manager.hpp"
#include "core/partition/tracker.hpp"
#include "core/executor/executor.hpp"
#include "core/shuffle_meta.hpp"
#include "comm/abstract_sender.hpp"
#include "core/collection_map.hpp"

namespace xyz {

struct JoinMeta {
  int part_id;
  int upstream_part_id;
  std::function<void(std::shared_ptr<AbstractPartition>)> func;
};

/*
 * A thread-safe structure to protect the partitions.
 */
class PartitionTracker {
 public:
  PartitionTracker(int node_id,
                   std::shared_ptr<PartitionManager> partition_manager,
                   std::shared_ptr<Executor> executor,
                   std::shared_ptr<AbstractSender> sender,
                   std::shared_ptr<CollectionMap> collection_map)
      : node_id_(node_id), partitions_(partition_manager), executor_(executor), 
        sender_(sender), collection_map_(collection_map) {}

  void SetPlan(PlanSpec plan);
  PlanSpec GetPlan() { return plan_; }

  // Called by map
  void RunAllMap(std::function<void(ShuffleMeta, std::shared_ptr<AbstractPartition>, 
                                    std::shared_ptr<AbstractMapProgressTracker>)> func);

  // Called by join
  void RunJoin(JoinMeta join_meta);

  std::shared_ptr<MapTracker> GetMapTracker() {
    return map_tracker_;
  }
  std::shared_ptr<JoinTracker> GetJoinTracker() {
    return join_tracker_;
  }
  bool FinishAllJoin();
 private:
  void StartMap(int part_id);
  void FinishMap(int part_id, std::shared_ptr<VersionedPartition> part);
  void StartJoin(int part_id, int upstream_part_id);
  void FinishJoin(int part_id, int upstream_part_id);

  void SendMapFinish();
  void SendJoinFinish();
 private:
  std::shared_ptr<PartitionManager> partitions_;
  PlanSpec plan_;

  std::condition_variable cond_;
  std::mutex mu_;
  bool plan_set_ = false;
  std::set<int> unassigned_map_parts_;
  std::set<int> unfinished_map_parts_;
  std::map<int, std::vector<JoinMeta>> pending_join_;

  std::shared_ptr<JoinTracker> join_tracker_;
  std::shared_ptr<MapTracker> map_tracker_;

  std::shared_ptr<Executor> executor_;
  std::shared_ptr<AbstractSender> sender_;
  std::shared_ptr<CollectionMap> collection_map_;
  int node_id_;
};

}  // namespace

