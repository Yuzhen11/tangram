#include "gtest/gtest.h"
#include "glog/logging.h"

#include "core/partition/partition_manager.hpp"

namespace xyz {
namespace {

class TestPartitionManager : public testing::Test {};

template <typename T>
class FakePartition : public AbstractPartition {
  virtual void FromBin(SArrayBinStream& bin) override {}
  virtual void ToBin(SArrayBinStream& bin) override {}
  virtual size_t GetSize() const override { return 0; }
};

TEST_F(TestPartitionManager, Construct) {
  PartitionManager manager;
}

TEST_F(TestPartitionManager, Insert) {
  auto p1 = std::make_shared<FakePartition<int>>();
  auto p2 = std::make_shared<FakePartition<int>>();
  PartitionManager manager;
  manager.Insert(0, 0, std::move(p1));
  manager.Insert(0, 1, std::move(p2));
}

TEST_F(TestPartitionManager, InsertRemove) {
  auto p1 = std::make_shared<FakePartition<int>>();
  auto p2 = std::make_shared<FakePartition<int>>();
  PartitionManager manager;
  manager.Insert(0, 0, std::move(p1));
  manager.Insert(0, 1, std::move(p2));
  manager.Remove(0, 1);
}

TEST_F(TestPartitionManager, InsertGet) {
  auto p1 = std::make_shared<FakePartition<int>>();
  auto p2 = std::make_shared<FakePartition<int>>();
  PartitionManager manager;
  manager.Insert(0, 0, std::move(p1));
  manager.Insert(0, 1, std::move(p2));
  auto get_p1 = manager.Get(0, 1);
  EXPECT_EQ(get_p1.use_count(), 2);
  auto get_p2 = manager.Get(0, 1);
  EXPECT_EQ(get_p2.use_count(), 3);
}

}  // namespace
}  // namespace xyz

