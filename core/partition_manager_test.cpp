#include "gtest/gtest.h"
#include "glog/logging.h"

#include "core/partition_manager.hpp"

namespace xyz {
namespace {

class TestPartitionManager : public testing::Test {};

template <typename T>
class FakePartition : public AbstractPartition {
};

TEST_F(TestPartitionManager, Construct) {
  PartitionManager manager;
}

TEST_F(TestPartitionManager, Insert) {
  auto* p1 = new FakePartition<int>;
  auto* p2 = new FakePartition<int>;
  PartitionManager manager;
  manager.Insert(0, 0, p1);
  manager.Insert(0, 1, p2);
}

TEST_F(TestPartitionManager, InsertGet) {
  auto* p1 = new FakePartition<int>;
  auto* p2 = new FakePartition<int>;
  PartitionManager manager;
  manager.Insert(0, 0, p1);
  manager.Insert(0, 1, p2);
  {
    auto item1 = manager.Get(0, 1);
    auto item2 = manager.Get(0, 1);
    EXPECT_EQ(*item2.p_count, 2);
    EXPECT_EQ(*item2.p_count, *item1.p_count);
    EXPECT_EQ(item2.partition, item1.partition);
    EXPECT_EQ(manager.CheckCount(0, 1), 2);
  }
  EXPECT_EQ(manager.CheckCount(0, 1), 0);
}
TEST_F(TestPartitionManager, InsertRemove) {
  auto* p1 = new FakePartition<int>;
  auto* p2 = new FakePartition<int>;
  PartitionManager manager;
  manager.Insert(0, 0, p1);
  manager.Insert(0, 1, p2);
  manager.Remove(0, 0);
  manager.Remove(0, 1);
}

}  // namespace
}  // namespace xyz

