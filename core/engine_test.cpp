#include "gtest/gtest.h"
#include "glog/logging.h"

#include "core/engine.hpp"
#include "core/partition/partition_manager.hpp"

namespace xyz {
namespace {

class TestEngine : public testing::Test {};

class FakeMapOutput : public AbstractMapOutput {
 public:
  virtual std::vector<SArrayBinStream> Serialize() override {}
  virtual void Combine() override {};
};

template <typename T>
class FakePartition : public AbstractPartition {
};

TEST_F(TestEngine, Create) {
  const int thread_pool_size = 4;
  Engine engine(thread_pool_size);
}

TEST_F(TestEngine, AddPlanItem) {
  const int thread_pool_size = 4;
  Engine engine(thread_pool_size);

  /*
  PlanItem::MapFuncT map = [](std::shared_ptr<AbstractPartition>, std::shared_ptr<AbstractMapOutput>) {
    LOG(INFO) << "Map";
  };
  PlanItem::JoinFuncT join = [](std::shared_ptr<AbstractPartition>) {
    LOG(INFO) << "join";
  };
  const int plan_id = 0;
  PlanItem plan(plan_id, 0, 0, map, join);
  engine.AddPlan(plan_id, plan);
  */
}

}  // namespace
}  // namespace xyz

