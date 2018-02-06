#pragma once

#include "core/partition/abstract_partition.hpp"

namespace xyz {

struct VersionedPartition {
  int version;
  std::shared_ptr<AbstractPartition> partition;
};

}  // namespace

