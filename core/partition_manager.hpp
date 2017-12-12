#pragma once

#include "core/abstract_partition_manager.hpp"

#include "core/abstract_partition.hpp"

#include <map>

namespace xyz {

// Not thread-safe
class PartitionManager : public AbstractPartitionManager {
 public:
  using AbstractPartitionManager::PartitionItem;
  virtual ~PartitionManager();
  /*
   * Get a partition
   */
  virtual PartitionItem Get(int collection_id, int partition_id) override;
  /*
   * Insert a partition
   */
  virtual void Insert(int collection_id, int partition_id, AbstractPartition*) override;

  virtual void Remove(int collection_id, int partition_id) override;

  // For debug use.
  int CheckCount(int collection_id, int partition_id) {
    return partition_count_[collection_id][partition_id];
  }
 private:
  // <collection_id, <partition_id, partition>>
  // Let PartitionManager own the partition.
  std::map<int, std::map<int, AbstractPartition*>> partitions_;
  std::map<int, std::map<int, int>> partition_count_;
};

}  // namespace

