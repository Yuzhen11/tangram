#include "gtest/gtest.h"
#include "glog/logging.h"

#include "core/plan/plan_with.hpp"

namespace xyz {
namespace {

struct ObjT {
  using KeyT = int;
  using ValT = int;
  KeyT Key() const { return a; }
  int a;
};

class TestPlanWith: public testing::Test {};

TEST_F(TestPlanWith, Create) {
  int plan_id = 0;
  Collection<ObjT> c1{1};
  Collection<ObjT> c2{2};
  Collection<ObjT> c3{4};
  PlanWith<ObjT, ObjT, int, ObjT> plan(plan_id, c1, c2, c3);
  plan.mapwith = [](const ObjT& obj, TypedCache<ObjT>* cache) {
    ObjT cache_obj = cache->Get(2);
    int ret = obj.Key() + cache_obj.a;
    return std::pair<ObjT::KeyT, int>(ret, 1);
  };
}

}  // namespace
}  // namespace xyz

