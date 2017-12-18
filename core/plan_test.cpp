#include "gtest/gtest.h"
#include "glog/logging.h"

#include "core/plan.hpp"

namespace xyz {
namespace {

class TestPlan: public testing::Test {};

struct ObjT {
  using KeyT = int;
  int a;
};

TEST_F(TestPlan, Create) {
  int plan_id = 0;
  Collection<int> c1{1};
  Collection<ObjT> c2{2};
  Plan<int, ObjT, int> plan(plan_id, c1, c2);

  auto map = [](int a) {
    return std::pair<ObjT::KeyT, int>(a, 1);
  };
  auto join = [](int a, int m) {
    return a + m;
  };
  plan.SetMap(map);
  plan.SetJoin(join);
}

TEST_F(TestPlan, GetPlanItem) {
  int plan_id = 0;
  Collection<int> c1{1};
  Collection<ObjT> c2{2};
  Plan<int, ObjT, int> plan(plan_id, c1, c2);

  auto map = [](int a) {
    return std::pair<ObjT::KeyT, int>(a, 1);
  };
  auto join = [](int a, int m) {
    return a + m;
  };
  plan.SetMap(map);
  plan.SetJoin(join);
  PlanItem plan_item = plan.GetPlanItem();
  auto partition = std::make_shared<Partition<int>>();
  partition->Add(10);
  partition->Add(20);
  auto output_manager = std::make_shared<OutputManager<int, int>>();
  plan_item.map(partition, output_manager);
  auto output = output_manager->Get();
  ASSERT_EQ(output.size(), 2);
  EXPECT_EQ(output[0], std::make_pair(10, 1));
  EXPECT_EQ(output[1], std::make_pair(20, 1));
}

}  // namespace
}  // namespace xyz

