#include "core/partition_manager.hpp"

#include "glog/logging.h"

namespace xyz {

PartitionManager::~PartitionManager() {
  for (auto& kv: partitions_) {
    for (auto& inner_kv: kv.second) {
      CHECK(inner_kv.second);
      VLOG_IF(1, (partitions_[kv.first][inner_kv.first].use_count() > 1))
          << "some partitions are referenced when the PartitionManager is destroying.";
      // CHECK_EQ(partitions_[kv.first][inner_kv.first].use_count(), 1) 
      //     << "cannot remove (collection_id, partition_id):(" << kv.first << "," << inner_kv.first 
      //     << "), count:" << partitions_[kv.first][inner_kv.first].use_count() << ", which is not 1.";
      partitions_[kv.first].erase(inner_kv.first);
    }
  }
}

std::shared_ptr<AbstractPartition> PartitionManager::Get(int collection_id, int partition_id) {
  CHECK(partitions_[collection_id].find(collection_id) != partitions_[collection_id].end());
  return partitions_[collection_id][partition_id];
}

void PartitionManager::Insert(int collection_id, int partition_id, std::shared_ptr<AbstractPartition>&& p) {
  CHECK(partitions_[collection_id].find(partition_id) == partitions_[collection_id].end());
  CHECK_EQ(p.use_count(), 1) << "the partition has only one reference.";
  partitions_[collection_id][partition_id] = std::move(p);
}

void PartitionManager::Remove(int collection_id, int partition_id) {
  VLOG_IF(1, (partitions_[collection_id][partition_id].use_count() > 1))
      << "some partitions are referenced when the PartitionManager is destroying.";
  // CHECK_EQ(partitions_[collection_id][partition_id].use_count(), 1) 
  //     << "cannot remove (collection_id, partition_id):(" << collection_id << "," << partition_id 
  //     << "), count:" << partitions_[collection_id][partition_id].use_count() << ", which is not 1.";
  partitions_[collection_id].erase(partition_id);
  if (partitions_[collection_id].size() == 0) partitions_.erase(collection_id);
}

}  // namespace

