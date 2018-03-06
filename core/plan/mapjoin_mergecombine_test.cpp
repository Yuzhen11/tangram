#include "gtest/gtest.h"
#include "glog/logging.h"

#include "core/plan/mapjoin_mergecombine.hpp"

namespace xyz {
namespace {

class TestMapJoinMergeCombine : public testing::Test {};

struct ObjT {
  using KeyT = int;
  using ValT = int;
  ObjT() = default;
  ObjT(KeyT key) : a(key), b(0) {}
  KeyT Key() const { return a; }
  int a;
  int b;
};

TEST_F(TestMapJoinMergeCombine, Create) {
  int plan_id = 0;
  Collection<ObjT> c1{1};
  Collection<ObjT> c2{2};
  auto plan = GetMapJoinMergeCombine<int>(plan_id, &c1, &c2);
}

}  // namespace
}  // namespace xyz

