#include "gtest/gtest.h"
#include "glog/logging.h"

#include "core/partition/indexed_seq_partition.hpp"

namespace xyz {
namespace {

class TestIndexedSeqPartition : public testing::Test {};

struct ObjT {
  using KeyT = int;
  using ValT = int;
  ObjT() = default;
  ObjT(KeyT _key):key(_key) {}
  ObjT(KeyT _key, ValT _val):key(_key), val(_val) {}
  int key;
  int val;
  KeyT Key() const { return key; }
};

TEST_F(TestIndexedSeqPartition, Create) {
  IndexedSeqPartition<ObjT> part;
}

TEST_F(TestIndexedSeqPartition, Add) {
  IndexedSeqPartition<ObjT> part;
  part.Add(ObjT{1, 2});
  part.Add(ObjT{2, 3});
  EXPECT_EQ(part.GetSize(), 2);
  EXPECT_EQ(part.GetUnsortedSize(), 2);
  EXPECT_EQ(part.GetSortedSize(), 0);
  part.Sort();
  EXPECT_EQ(part.GetSize(), 2);
  EXPECT_EQ(part.GetUnsortedSize(), 0);
  EXPECT_EQ(part.GetSortedSize(), 2);
}

TEST_F(TestIndexedSeqPartition, Get) {
  IndexedSeqPartition<ObjT> part;
  part.Add(ObjT{1, 2});
  part.Add(ObjT{5, 3});
  part.Add(ObjT{4, 4});
  part.Add(ObjT{2, 5});
  part.Add(ObjT{3, 6});
  part.Sort();
  EXPECT_EQ(part.Get(5).val, 3);
  EXPECT_EQ(part.Get(4).val, 4);
}

}  // namespace
}  // namespace xyz

