#pragma once

#include "core/abstract_partition_manager.hpp"

#include "core/abstract_partition.hpp"

#include <map>

namespace xyz {

// Not thread-safe
class PartitionManager : public AbstractPartitionManager {
 public:
  virtual ~PartitionManager();
  /*
   * Get a partition
   */
  virtual std::shared_ptr<AbstractPartition> Get(int collection_id, int partition_id) override;
  /*
   * Insert a partition
   */
  virtual void Insert(int collection_id, int partition_id, std::shared_ptr<AbstractPartition>&&) override;

  virtual void Remove(int collection_id, int partition_id) override;
 private:
  // <collection_id, <partition_id, partition>>
  // Let PartitionManager own the partition.
  std::map<int, std::map<int, std::shared_ptr<AbstractPartition>>> partitions_;
};

}  // namespace

