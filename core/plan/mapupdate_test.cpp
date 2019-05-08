#include "gtest/gtest.h"
#include "glog/logging.h"

#include "core/plan/mapupdate.hpp"
#include "core/partition/seq_partition.hpp"
#include "core/map_output/partitioned_map_output.hpp"

namespace xyz {
namespace {

/*
 * This test depends on SeqPartition and MapOutput.
 */
class TestMapJoin: public testing::Test {};

struct ObjT {
  using KeyT = int;
  using ValT = int;
  ObjT() = default;
  ObjT(KeyT key) : a(key), b(0) {}
  KeyT Key() const { return a; }
  int a;
  int b;
};

TEST_F(TestMapJoin, Create) {
  int plan_id = 0;
  Collection<ObjT> c1{1};
  Collection<ObjT> c2{2};
  auto plan = GetMapJoin<int>(plan_id, &c1, &c2);

  plan.map = [](ObjT a, Output<typename ObjT::KeyT, int>* o) {
    o->Add(a.Key(), 1);
  };
  plan.update = [](ObjT* obj, int m) {
    obj->b += m;
  };
}

TEST_F(TestMapJoin, GetMapPartFunc) {
  int plan_id = 0;
  int num_part = 1;
  Collection<ObjT> c1{1};
  Collection<ObjT> c2{2, num_part};
  c2.SetMapper(std::make_shared<HashKeyToPartMapper<ObjT::KeyT>>(num_part));
  auto plan = GetMapJoin<int>(plan_id, &c1, &c2);

  plan.map = [](ObjT a, Output<typename ObjT::KeyT, int>* o) {
    o->Add(a.Key(), 1);
  };
  plan.SetMapPart();

  auto f = plan.GetMapPartFunc();
  auto partition = std::make_shared<SeqPartition<ObjT>>();
  partition->Add(ObjT{10});
  partition->Add(ObjT{20});
  auto map_output = f(partition);
  auto vec = static_cast<Output<int,int>*>(map_output.get())->GetBuffer();
  ASSERT_EQ(vec.size(), 1);
  ASSERT_EQ(vec[0].size(), 2);
  EXPECT_EQ(vec[0][0].first, 10);
  EXPECT_EQ(vec[0][0].second, 1);
  EXPECT_EQ(vec[0][1].first, 20);
  EXPECT_EQ(vec[0][1].second, 1);
}

}  // namespace
}  // namespace xyz

