#pragma once

#include "base/sarray_binstream.hpp"

namespace xyz {

class AbstractPartitionCache {
 public:
  virtual ~AbstractPartitionCache() = default;
  virtual void UpdatePartition(int collection_id, int partition_id, int version, SArrayBinStream bin) = 0;
};

}  // namespace xyz

