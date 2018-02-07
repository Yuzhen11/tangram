#include "gtest/gtest.h"
#include "glog/logging.h"

#include "core/plan/plan.hpp"
#include "core/partition/seq_partition.hpp"
#include "core/map_output/partitioned_map_output.hpp"

namespace xyz {
namespace {

/*
 * This test depends on SeqPartition and MapOutput.
 */
class TestPlan: public testing::Test {};

struct ObjT {
  using KeyT = int;
  using ValT = int;
  ObjT(KeyT key) : a(key), b(0) {}
  KeyT Key() const { return a; }
  int a;
  int b;
};

TEST_F(TestPlan, Create) {
  int plan_id = 0;
  Collection<ObjT> c1{1};
  Collection<ObjT> c2{2};
  Plan<ObjT, ObjT, int> plan(plan_id, c1, c2);

  plan.map = [](ObjT a) {
    return std::pair<ObjT::KeyT, int>(a.Key(), 1);
  };
  plan.join = [](ObjT* obj, int m) {
    obj->b += m;
  };
}

/*
TEST_F(TestPlan, GetPlanItem) {
  int plan_id = 0;
  int num_part = 4;
  Collection<ObjT> c1{1, nullptr};
  Collection<ObjT> c2{2, std::make_shared<HashKeyToPartMapper<ObjT::KeyT>>(num_part)};
  auto partition = std::make_shared<SeqPartition<ObjT>>();
  partition->Add(ObjT{10});
  partition->Add(ObjT{20});

  Plan<ObjT, ObjT, int> plan(plan_id, c1, c2);
  plan.map = [](ObjT a) {
    return std::pair<ObjT::KeyT, int>(a.Key(), 1);
  };
  plan.join = [](int a, int m) {
    return a + m;
  };
  PlanItem plan_item = plan.GetPlanItem();
  auto map_output = plan_item.map(partition);
  auto output = static_cast<PartitionedMapOutput<int,int>*>(map_output.get())->GetBuffer();
  ASSERT_EQ(output.size(), num_part);
}
*/

}  // namespace
}  // namespace xyz

