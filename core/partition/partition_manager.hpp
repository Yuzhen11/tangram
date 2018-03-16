#pragma once

#include "core/partition/abstract_partition.hpp"

#include "glog/logging.h"

#include <mutex>
#include <memory>
#include <map>

namespace xyz {

class PartitionManager {
 public:
  PartitionManager() = default;
  ~PartitionManager();

  bool Has(int collection_id, int partition_id);
  std::shared_ptr<AbstractPartition> Get(int collection_id, int partition_id);

  std::vector<std::shared_ptr<AbstractPartition>> Get(int collection_id);
  int GetNumLocalParts(int collection_id);

  void Insert(int collection_id, int partition_id, std::shared_ptr<AbstractPartition>&&);

  void Remove(int collection_id, int partition_id);
 private:
  // <collection_id, <partition_id, partition>>
  // Let PartitionManager own the partition.
  std::map<int, std::map<int, std::shared_ptr<AbstractPartition>>> partitions_;
  // Make it thread-safe
  std::mutex mu_;
};

}  // namespace

