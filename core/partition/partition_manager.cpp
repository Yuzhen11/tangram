#include "core/partition/partition_manager.hpp"

#include "glog/logging.h"

namespace xyz {

PartitionManager::~PartitionManager() {
  std::lock_guard<std::mutex> lk(mu_);
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

std::shared_ptr<VersionedPartition> PartitionManager::Get(int collection_id, int partition_id) {
  std::lock_guard<std::mutex> lk(mu_);
  CHECK(partitions_[collection_id].find(partition_id) != partitions_[collection_id].end());
  return partitions_[collection_id][partition_id];
}

std::shared_ptr<VersionedPartition> PartitionManager::Get(int collection_id, int partition_id, int version) {
  std::lock_guard<std::mutex> lk(mu_);
  CHECK(partitions_[collection_id].find(partition_id) != partitions_[collection_id].end());
  // TODO Now do not allow requested version to be smaller
  CHECK_LE(version, partitions_[collection_id][partition_id]->version);
  return partitions_[collection_id][partition_id];
}

// For test
bool PartitionManager::Has(int collection_id, int partition_id) {
  std::lock_guard<std::mutex> lk(mu_);
  if (partitions_.find(collection_id) == partitions_.end()
          || partitions_[collection_id].find(partition_id) == partitions_[collection_id].end()) {
    return false;
  } else {
    return true;
  }
}

bool PartitionManager::Has(int collection_id, int partition_id, int version) {
  std::lock_guard<std::mutex> lk(mu_);
  if (partitions_.find(collection_id) == partitions_.end()
          || partitions_[collection_id].find(partition_id) == partitions_[collection_id].end()) {
    return false;
  } else {
    return version <= partitions_[collection_id][partition_id]->version;
  }
}

std::vector<std::shared_ptr<VersionedPartition>> PartitionManager::Get(int collection_id) {
  std::lock_guard<std::mutex> lk(mu_);
  std::vector<std::shared_ptr<VersionedPartition>> ret;
  for (auto& part : partitions_[collection_id]) {
    ret.push_back(part.second);
  }
  return ret;
}

int PartitionManager::GetNumLocalParts(int collection_id) {
  std::lock_guard<std::mutex> lk(mu_);
  return partitions_[collection_id].size();
}

void PartitionManager::Insert(int collection_id, int partition_id, std::shared_ptr<AbstractPartition>&& p) {
  std::lock_guard<std::mutex> lk(mu_);
  CHECK(partitions_[collection_id].find(partition_id) == partitions_[collection_id].end());
  CHECK_EQ(p.use_count(), 1) << "the partition has only one reference.";
  auto part = std::make_shared<VersionedPartition>();
  part->version = 0;
  part->partition = std::move(p);
  part->part_id = partition_id;
  partitions_[collection_id][partition_id] = std::move(part);
}

void PartitionManager::Remove(int collection_id, int partition_id) {
  std::lock_guard<std::mutex> lk(mu_);
  partitions_[collection_id].erase(partition_id);
  if (partitions_[collection_id].size() == 0) partitions_.erase(collection_id);
}

}  // namespace

