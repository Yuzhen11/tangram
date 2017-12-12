#include "core/partition_manager.hpp"

#include "glog/logging.h"

namespace xyz {

PartitionManager::~PartitionManager() {
  for (auto& kv: partitions_) {
    for (auto& inner_kv: kv.second) {
      CHECK(inner_kv.second);
      CHECK_EQ(partition_count_[kv.first][inner_kv.first], 0) 
          << "cannot remove (collection_id, partition_id):(" << kv.first << "," << inner_kv.first 
          << "), count:" << partition_count_[kv.first][inner_kv.first] << ", which is not 0.";
      delete inner_kv.second;
    }
  }
}

PartitionManager::PartitionItem PartitionManager::Get(int collection_id, int partition_id) {
  CHECK(partitions_[collection_id].find(collection_id) != partitions_[collection_id].end());
  CHECK(partition_count_[collection_id].find(collection_id) != partition_count_[collection_id].end());
  PartitionItem item(partitions_[collection_id][partition_id], &partition_count_[collection_id][partition_id]);
  return std::move(item);  // Have to use move to return since copy constructor is disabled.
}

void PartitionManager::Insert(int collection_id, int partition_id, AbstractPartition* p) {
  CHECK(partitions_[collection_id].find(partition_id) == partitions_[collection_id].end());
  CHECK(partition_count_[collection_id].find(partition_id) == partition_count_[collection_id].end());
  partitions_[collection_id][partition_id] = p;
  partition_count_[collection_id][partition_id] = 0;
}

void PartitionManager::Remove(int collection_id, int partition_id) {
  CHECK_EQ(partition_count_[collection_id][partition_id], 0) 
      << "cannot remove (collection_id, partition_id):(" << collection_id << "," << partition_id 
      << "), count:" << partition_count_[collection_id][partition_id] << ", which is not 0.";
  partition_count_[collection_id].erase(partition_id);
  if (partition_count_[collection_id].size() == 0) partition_count_.erase(collection_id);
  CHECK(partitions_[collection_id][partition_id] != nullptr);
  delete partitions_[collection_id][partition_id];
  partitions_[collection_id].erase(partition_id);
  if (partitions_[collection_id].size() == 0) partitions_.erase(collection_id);
}

}  // namespace

