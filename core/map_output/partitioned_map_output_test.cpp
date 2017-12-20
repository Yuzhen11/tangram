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


}  // namespace
}  // namespace xyz

