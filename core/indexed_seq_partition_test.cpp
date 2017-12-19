#include "gtest/gtest.h"
#include "glog/logging.h"

#include "core/indexed_seq_partition.hpp"

namespace xyz {
namespace {

class TestIndexedSeqPartition : public testing::Test {};

struct ObjT {
  using KeyT = int;
  using ValT = int;
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

}  // namespace
}  // namespace xyz

