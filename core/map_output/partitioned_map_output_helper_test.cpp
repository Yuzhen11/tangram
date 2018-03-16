#include "gtest/gtest.h"
#include "glog/logging.h"

#include "core/map_output/partitioned_map_output_helper.hpp"

// This test depends on HashKeyToPartMapper.
#include "core/index/hash_key_to_part_mapper.hpp"

namespace xyz {
namespace {

class TestPartitionedMapOutputHelper : public testing::Test {};

TEST_F(TestPartitionedMapOutputHelper, MergeCombineMultipleMapOutput) {
  auto mapper = std::make_shared<HashKeyToPartMapper<int>>(1);
  std::vector<std::shared_ptr<AbstractMapOutput>> map_outputs(3);
  for (auto& map_output : map_outputs) {
    map_output.reset(new PartitionedMapOutput<int, int>(mapper));
    static_cast<PartitionedMapOutput<int, int>*>(map_output.get())->SetCombineFunc([](int a, int b) { return a + b; });
  }
  static_cast<PartitionedMapOutput<int, int>*>(map_outputs[0].get())->Add({{2, 1}, {3, 2}});
  static_cast<PartitionedMapOutput<int, int>*>(map_outputs[1].get())->Add({{3, 3}, {2, 2}});
  static_cast<PartitionedMapOutput<int, int>*>(map_outputs[2].get())->Add({{1, 4}, {3, 2}});
  SArrayBinStream bin = MergeCombineMultipleMapOutput<int, int>(map_outputs, 0);
  int a, b;
  bin >> a >> b;
  EXPECT_EQ(a, 1);
  EXPECT_EQ(b, 4);
  bin >> a >> b;
  EXPECT_EQ(a, 2);
  EXPECT_EQ(b, 3);
  bin >> a >> b;
  EXPECT_EQ(a, 3);
  EXPECT_EQ(b, 7);
}

}  // namespace
}  // namespace xyz

