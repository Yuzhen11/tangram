#pragma once

#include "core/abstract_partition.hpp"

#include "glog/logging.h"

namespace xyz {

/*
 * The PartitionItem is designed to be reference-counted in a non threadsafe manner.
 */
class AbstractPartitionManager {
 public:
  // Not threadsafe
  struct PartitionItem {
    AbstractPartition* partition;
    int* p_count;
    PartitionItem(AbstractPartition* p, int* c): partition(p), p_count(c) {
      CHECK(partition);
      CHECK(p_count);
      *p_count += 1;
    }
    ~PartitionItem() {
      *p_count -= 1;
    }
    PartitionItem(const PartitionItem&) = delete;
    PartitionItem& operator=(const PartitionItem&) = delete;
    PartitionItem(PartitionItem&& other): partition(other.partition), p_count(other.p_count) {
      *p_count += 1;
    }
    PartitionItem& operator=(PartitionItem&& other) {
      partition = other.partition;
      p_count = other.p_count;
      *p_count += 1;
    }
  };
  virtual ~AbstractPartitionManager() {}

  virtual PartitionItem Get(int collection_id, int partition_id) = 0;
  virtual void Insert(int collection_id, int partition_id, AbstractPartition*) = 0;
  virtual void Remove(int collection_id, int partition_id) = 0;
};

}  // namespace

