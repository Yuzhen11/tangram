#include "gtest/gtest.h"
#include "glog/logging.h"

#include "core/partition/seq_partition.hpp"

namespace xyz {
namespace {

class TestSeqPartition : public testing::Test {};

struct ObjT {
  using KeyT = int;
  using ValT = int;
  int key;
  int val;
  KeyT Key() const { return key; }
};

TEST_F(TestSeqPartition, Create) {
  SeqPartition<ObjT> part;
}
TEST_F(TestSeqPartition, Add) {
  SeqPartition<ObjT> part;
  part.Add(ObjT{2, 3});
  part.Add(ObjT{1, 2});
  EXPECT_EQ(part.GetSize(), 2);
}

TEST_F(TestSeqPartition, EmptyIterate) {
  SeqPartition<ObjT> part;
  for (auto& elem : part) {
  }
}

TEST_F(TestSeqPartition, Iterate) {
  SeqPartition<ObjT> part;
  std::vector<ObjT> v{ObjT{1, 2}, ObjT{2, 3}};
  part.Add(v[0]);
  part.Add(v[1]);
  ASSERT_EQ(part.GetSize(), 2);
  int i = 0;
  for (auto& elem : part) {
    EXPECT_EQ(elem.Key(), v[i].Key());
    i ++;
  }
}

TEST_F(TestSeqPartition, Bin) {
  SeqPartition<ObjT> part;
  part.Add(ObjT{2, 3});
  part.Add(ObjT{1, 2});
  SArrayBinStream bin;
  part.ToBin(bin);
  SeqPartition<ObjT> new_part;
  new_part.FromBin(bin);
  ASSERT_EQ(new_part.GetSize(), 2);
}

}  // namespace
}  // namespace xyz

