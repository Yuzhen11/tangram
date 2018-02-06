#pragma once

#include "base/sarray_binstream.hpp"
#include "core/partition/versioned_partition.hpp"

namespace xyz {

class AbstractPartitionCache {
 public:
  virtual ~AbstractPartitionCache() = default;
  virtual void UpdatePartition(
          int collection_id, 
          int partition_id, 
          int version, 
          SArrayBinStream bin) = 0;

  virtual std::shared_ptr<VersionedPartition> GetPartition(
          int collection_id, 
          int partition_id, 
          int version) = 0;
};

}  // namespace xyz

