#pragma once

#include "core/partition/abstract_partition.hpp"
#include "core/partition/partition_manager.hpp"

#include "glog/logging.h"

#include <memory>
#include <map>

namespace xyz {

// Not thread-safe
/*
 * The PartitionItem is designed to be reference-counted in a non threadsafe manner.
 */
class PartitionManager {
 public:
  PartitionManager() = default;
  ~PartitionManager();

  std::shared_ptr<AbstractPartition> Get(int collection_id, int partition_id);

  const std::map<int, std::shared_ptr<AbstractPartition>>& Get(int collection_id);

  void Insert(int collection_id, int partition_id, std::shared_ptr<AbstractPartition>&&);

  void Remove(int collection_id, int partition_id);
 private:
  // <collection_id, <partition_id, partition>>
  // Let PartitionManager own the partition.
  std::map<int, std::map<int, std::shared_ptr<AbstractPartition>>> partitions_;
};

}  // namespace

