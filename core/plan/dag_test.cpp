#include "gtest/gtest.h"
#include "glog/logging.h"

#include "core/plan/dag.hpp"

namespace xyz {
namespace {

class TestDag : public testing::Test {};

TEST_F(TestDag, construct) {
  Dag d;
}

TEST_F(TestDag, AddDagNode) {
  Dag d;
  d.AddDagNode(0, {}, {0});
  d.AddDagNode(1, {}, {1});
  d.AddDagNode(2, {0}, {1});
  LOG(INFO) << d.DebugString();
}

TEST_F(TestDag, Vistor) {
  Dag d;
  d.AddDagNode(0, {}, {0});
  d.AddDagNode(1, {}, {1});
  d.AddDagNode(2, {0}, {1});

  DagVistor v(d);
  auto f = v.GetFront();
  while (!f.empty()) {
    int node = f.front();
    LOG(INFO) << "visiting: " << node;
    v.Finish(node);
    f = v.GetFront();
  }
}

}  // namespace
}  // namespace xyz

