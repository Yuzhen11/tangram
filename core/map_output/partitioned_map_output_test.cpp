#include "gtest/gtest.h"
#include "glog/logging.h"

#include "core/map_output/partitioned_map_output.hpp"

// This test depends on HashKeyToPartMapper.
#include "core/index/hash_key_to_part_mapper.hpp"

namespace xyz {
namespace {

class TestPartitionedMapOutput : public testing::Test {};

TEST_F(TestPartitionedMapOutput, Construct) {
  auto mapper = std::make_shared<HashKeyToPartMapper<std::string>>(4);
  PartitionedMapOutput<std::string, int> output(mapper);
}

TEST_F(TestPartitionedMapOutput, Add) {
  auto mapper = std::make_shared<HashKeyToPartMapper<std::string>>(4);
  PartitionedMapOutput<std::string, int> output(mapper);
  output.Add({"abc", 1});
  output.Add({"hello", 2});
  auto buffer = output.GetBuffer();
  for (auto b : buffer) {
    VLOG(1) << b.size();
  }
  ASSERT_EQ(buffer.size(), 4);
  EXPECT_EQ(buffer[0].size(), 1);
  EXPECT_EQ(buffer[1].size(), 1);
  EXPECT_EQ(buffer[2].size(), 0);
  EXPECT_EQ(buffer[3].size(), 0);
}

TEST_F(TestPartitionedMapOutput, AddVector) {
  auto mapper = std::make_shared<HashKeyToPartMapper<std::string>>(4);
  PartitionedMapOutput<std::string, int> output(mapper);
  std::vector<std::pair<std::string, int>> v{{"abc", 1}, {"hello", 2}};
  output.Add(v);
  auto buffer = output.GetBuffer();
  for (auto b : buffer) {
    VLOG(1) << b.size();
  }
  ASSERT_EQ(buffer.size(), 4);
  EXPECT_EQ(buffer[0].size(), 1);
  EXPECT_EQ(buffer[1].size(), 1);
  EXPECT_EQ(buffer[2].size(), 0);
  EXPECT_EQ(buffer[3].size(), 0);
}

TEST_F(TestPartitionedMapOutput, CombineOneBuffer) {
  std::vector<std::pair<int, int>> buffer{{3, 1}, {2, 1}, {2, 1}, {3, 3}, {3, 2}};
  auto combine = [](int a, int b) { return a + b; };
  const std::vector<std::pair<int, int>> expected{{3, 1}, {2, 2}, {3, 5}};
  PartitionedMapOutput<int, int>::CombineOneBuffer(buffer, combine);
  ASSERT_EQ(buffer.size(), expected.size());
  for (int i = 0; i < buffer.size(); ++ i) {
    EXPECT_EQ(buffer[i], expected[i]);
  }
}

TEST_F(TestPartitionedMapOutput, Combine) {
  auto mapper = std::make_shared<HashKeyToPartMapper<int>>(1);
  PartitionedMapOutput<int, int> output(mapper);
  std::vector<std::pair<int, int>> v{{3, 1}, {2, 1}, {2, 1}, {3, 3}, {3, 2}};
  output.Add(v);
  output.SetCombineFunc([](int a, int b) { return a + b; });
  output.Combine();
  auto buffer = output.GetBuffer();
  ASSERT_EQ(buffer.size(), 1);
  const std::vector<std::pair<int, int>> expected{{2, 2}, {3, 6}};
  for (int i = 0; i < buffer[0].size(); ++ i) {
    EXPECT_EQ(buffer[0][i], expected[i]);
  }
}

TEST_F(TestPartitionedMapOutput, Serialize) {
  auto mapper = std::make_shared<HashKeyToPartMapper<std::string>>(4);
  PartitionedMapOutput<std::string, int> output(mapper);
  std::vector<std::pair<std::string, int>> v{{"abc", 1}, {"hello", 2}};
  output.Add(v);
  auto bins = output.Serialize();
  std::vector<int> expected{17, 15, 0, 0};
  ASSERT_EQ(bins.size(), 4);
  for (int i = 0; i < bins.size(); ++ i) {
    VLOG(1) << "bin: " << bins[i].Size();
    EXPECT_EQ(expected[i], bins[i].Size());
  }
}

}  // namespace
}  // namespace xyz

