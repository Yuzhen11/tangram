#pragma once

#include <memory>

#include "core/abstract_partition.hpp"

#include "glog/logging.h"

namespace xyz {

/*
 * The PartitionItem is designed to be reference-counted in a non threadsafe manner.
 */
class AbstractPartitionManager {
 public:
  virtual ~AbstractPartitionManager() {}

  virtual std::shared_ptr<AbstractPartition> Get(int collection_id, int partition_id) = 0;
  virtual void Insert(int collection_id, int partition_id, std::shared_ptr<AbstractPartition>&&) = 0;
  virtual void Remove(int collection_id, int partition_id) = 0;
};

}  // namespace

