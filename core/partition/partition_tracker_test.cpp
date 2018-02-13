#include "gtest/gtest.h"
#include "glog/logging.h"

#include "core/partition/partition_tracker.hpp"

namespace xyz {
namespace {

class TestPartitionTracker : public testing::Test {};

TEST_F(TestPartitionTracker, Construct) {
  PartitionTracker tracker;
}

}  // namespace
}  // namespace xyz

