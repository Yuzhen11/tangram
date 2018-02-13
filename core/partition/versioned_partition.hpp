#pragma once

#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>

#include "core/partition/abstract_partition.hpp"

namespace xyz {

struct VersionedPartition {
  boost::shared_mutex mu;
  int part_id;
  int version;
  std::shared_ptr<AbstractPartition> partition;
};

}  // namespace

