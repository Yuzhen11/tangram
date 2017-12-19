#include "gtest/gtest.h"
#include "glog/logging.h"

#include "core/plan.hpp"
#include "core/seq_partition.hpp"
#include "core/map_output.hpp"

namespace xyz {
namespace {

/*
 * This test depends on SeqPartition and MapOutput.
 */
class TestPlan: public testing::Test {};

struct ObjT {
  using KeyT = int;
  using ValT = int;
  KeyT Key() const { return a; }
  int a;
};

TEST_F(TestPlan, Create) {
  int plan_id = 0;
  Collection<ObjT> c1{1};
  Collection<ObjT> c2{2};
  Plan<ObjT, ObjT, int> plan(plan_id, c1, c2);

  auto map = [](ObjT a) {
    return std::pair<ObjT::KeyT, int>(a.Key(), 1);
  };
  auto join = [](int a, int m) {
    return a + m;
  };
  plan.SetMap(map);
  plan.SetJoin(join);
}

TEST_F(TestPlan, GetPlanItem) {
  int plan_id = 0;
  Collection<ObjT> c1{1};
  Collection<ObjT> c2{2};
  Plan<ObjT, ObjT, int> plan(plan_id, c1, c2);

  auto map = [](ObjT a) {
    return std::pair<ObjT::KeyT, int>(a.Key(), 1);
  };
  auto join = [](int a, int m) {
    return a + m;
  };
  plan.SetMap(map);
  plan.SetJoin(join);
  PlanItem plan_item = plan.GetPlanItem();
  auto partition = std::make_shared<SeqPartition<ObjT>>();
  partition->Add(ObjT{10});
  partition->Add(ObjT{20});
  auto map_output = std::make_shared<MapOutput<int, int>>();
  plan_item.map(partition, map_output);
  auto output = map_output->Get();
  ASSERT_EQ(output.size(), 2);
  EXPECT_EQ(output[0], std::make_pair(10, 1));
  EXPECT_EQ(output[1], std::make_pair(20, 1));
}

}  // namespace
}  // namespace xyz

