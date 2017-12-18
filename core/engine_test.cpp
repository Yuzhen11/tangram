#include "gtest/gtest.h"
#include "glog/logging.h"

#include "core/engine.hpp"
#include "core/partition_manager.hpp"

namespace xyz {
namespace {

class TestEngine : public testing::Test {};

class FakeOutputManager : public AbstractOutputManager {
};

template <typename T>
class FakePartition : public AbstractPartition {
};

TEST_F(TestEngine, Create) {
  std::unique_ptr<AbstractPartitionManager> partition_manager(new PartitionManager);
  std::shared_ptr<AbstractOutputManager> output_manager(new FakeOutputManager);
  const int thread_pool_size = 4;
  Engine engine(thread_pool_size, std::move(partition_manager), std::move(output_manager));
}

TEST_F(TestEngine, AddPlanItem) {
  std::unique_ptr<AbstractPartitionManager> partition_manager(new PartitionManager);
  std::shared_ptr<AbstractOutputManager> output_manager(new FakeOutputManager);
  const int thread_pool_size = 4;
  Engine engine(thread_pool_size, std::move(partition_manager), std::move(output_manager));

  PlanItem::MapFuncT map = [](std::shared_ptr<AbstractPartition>, std::shared_ptr<AbstractOutputManager>) {
    LOG(INFO) << "Map";
  };
  PlanItem::JoinFuncT join = [](std::shared_ptr<AbstractPartition>) {
    LOG(INFO) << "join";
  };
  const int plan_id = 0;
  PlanItem plan(plan_id, 0, 0, map, join);
  engine.AddPlan(plan_id, plan);
}

TEST_F(TestEngine, RunPlanItem) {
  std::unique_ptr<AbstractPartitionManager> partition_manager(new PartitionManager);
  std::shared_ptr<AbstractOutputManager> output_manager(new FakeOutputManager);
  const int collection_id = 0;
  const int partition_id = 0;
  auto p1 = std::make_shared<FakePartition<int>>();
  partition_manager->Insert(0, 0, std::move(p1));

  const int thread_pool_size = 4;
  Engine engine(thread_pool_size, std::move(partition_manager), std::move(output_manager));

  PlanItem::MapFuncT map = [](std::shared_ptr<AbstractPartition>, std::shared_ptr<AbstractOutputManager>) {
    LOG(INFO) << "Map";
  };
  PlanItem::JoinFuncT join = [](std::shared_ptr<AbstractPartition>) {
    LOG(INFO) << "join";
  };
  const int plan_id = 0;
  PlanItem plan(plan_id, 0, 0, map, join);
  engine.AddPlan(plan_id, plan);

  const int phase = 0;
  engine.RunPlanItem(plan_id, phase, collection_id, partition_id);
  engine.RunPlanItem(plan_id, phase, collection_id, partition_id);
}

}  // namespace
}  // namespace xyz

