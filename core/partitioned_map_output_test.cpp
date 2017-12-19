#include "gtest/gtest.h"
#include "glog/logging.h"

#include "core/partitioned_map_output.hpp"

#include "core/abstract_key_to_part_mapper.hpp"
// This test depends on HashKeyToPartMapper.
#include "core/hash_key_to_part_mapper.hpp"

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


}  // namespace
}  // namespace xyz

