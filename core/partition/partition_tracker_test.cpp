#include "gtest/gtest.h"
#include "glog/logging.h"

#include "core/partition/partition_tracker.hpp"
#include "comm/simple_sender.hpp"

#include <numeric>
#include <future>
#include <thread>

namespace xyz {
namespace {

template <typename T>
struct FakePartition : public AbstractPartition {
  virtual void FromBin(SArrayBinStream& bin) override {}
  virtual void ToBin(SArrayBinStream& bin) override {}
  virtual size_t GetSize() const override { return objs.size(); }

  std::vector<T> objs;
};

class TestPartitionTracker : public testing::Test {};

TEST_F(TestPartitionTracker, Construct) {
  int id = 0;
  auto pm = std::make_shared<PartitionManager>();
  auto executor = std::make_shared<Executor>(1);
  auto sender = std::make_shared<SimpleSender>();
  auto cm = std::make_shared<CollectionMap>();
  PartitionTracker pt(id, pm, executor, sender, cm);
}

TEST_F(TestPartitionTracker, RunAllMap) {
  int id = 0;
  auto pm = std::make_shared<PartitionManager>();
  auto executor = std::make_shared<Executor>(1);
  auto p1 = std::make_shared<FakePartition<int>>();
  auto p2 = std::make_shared<FakePartition<int>>();
  auto p3 = std::make_shared<FakePartition<int>>();
  p1->objs.resize(100);
  std::iota(p1->objs.begin(), p1->objs.end(), 0);
  p2->objs.resize(100);
  std::iota(p2->objs.begin(), p2->objs.end(), 1000);
  p3->objs.resize(100);
  std::iota(p3->objs.begin(), p3->objs.end(), 10000);
  pm->Insert(0, 0, std::move(p1));
  pm->Insert(0, 1, std::move(p2));
  pm->Insert(0, 2, std::move(p3));

  auto sender = std::make_shared<SimpleSender>();
  auto cm = std::make_shared<CollectionMap>();
  CollectionView cv;
  cv.collection_id = 0;
  cv.num_partition = 3;
  cm->Insert(cv);
  PartitionTracker pt(id, pm, executor, sender, cm);
  PlanSpec plan;
  plan.plan_id = 0;
  plan.map_collection_id = 0;
  plan.join_collection_id = 1;
  pt.SetPlan(plan);
  std::vector<std::promise<void>> promises(6);
  std::vector<std::future<void>> futures;
  for (auto& p: promises) {
    futures.push_back(p.get_future());
  }
  int p_counter = 0;
  std::vector<std::promise<void>> promises2(6);
  std::vector<std::future<void>> futures2;
  for (auto& p: promises2) {
    futures2.push_back(p.get_future());
  }

  pt.RunAllMap([&promises, &futures2, &p_counter](ShuffleMeta meta, std::shared_ptr<AbstractPartition> p, 
                   std::shared_ptr<AbstractMapProgressTracker> t) {
    auto* part = static_cast<FakePartition<int>*>(p.get());
    int i = 0;
    for (auto elem : part->objs) {
      i += 1;
      if (i % 10 == 0) {
        t->Report(i);
        LOG(INFO) << "running " << i;
      }
      if (i % 50 == 0) {
        promises[p_counter].set_value();
        futures2[p_counter].wait();
        p_counter += 1;
      }
    }
  });
  std::thread th([&futures, &promises2, &pt]() {
    for (int i = 0; i < futures.size(); ++ i) {
      futures[i].wait();
      LOG(INFO) << "unblock ";
      auto map_tracker = pt.GetMapTracker();
      if (i == 0) {
        auto p = map_tracker->GetTaskTracker(0)->GetProgress();
        EXPECT_EQ(p.first, 50);
        EXPECT_EQ(p.second, 100);
        p = map_tracker->GetTaskTracker(1)->GetProgress();
        EXPECT_EQ(p.first, 0);
        EXPECT_EQ(p.second, 100);
        p = map_tracker->GetTaskTracker(2)->GetProgress();
        EXPECT_EQ(p.first, 0);
        EXPECT_EQ(p.second, 100);
      }
      if (i == 2) {
        auto p = map_tracker->GetTaskTracker(0)->GetProgress();
        EXPECT_EQ(p.first, 100);
        EXPECT_EQ(p.second, 100);
        p = map_tracker->GetTaskTracker(1)->GetProgress();
        EXPECT_EQ(p.first, 50);
        EXPECT_EQ(p.second, 100);
        p = map_tracker->GetTaskTracker(2)->GetProgress();
        EXPECT_EQ(p.first, 0);
        EXPECT_EQ(p.second, 100);
      }
      promises2[i].set_value();
    }
  });
  th.join();
}

TEST_F(TestPartitionTracker, RunAllMapAndJoin) {
  int id = 0;
  auto pm = std::make_shared<PartitionManager>();
  auto executor = std::make_shared<Executor>(1);
  auto p1 = std::make_shared<FakePartition<int>>();
  auto p2 = std::make_shared<FakePartition<int>>();
  auto p3 = std::make_shared<FakePartition<int>>();
  p1->objs.resize(100);
  std::iota(p1->objs.begin(), p1->objs.end(), 0);
  p2->objs.resize(100);
  std::iota(p2->objs.begin(), p2->objs.end(), 1000);
  p3->objs.resize(100);
  std::iota(p3->objs.begin(), p3->objs.end(), 10000);
  pm->Insert(0, 0, std::move(p1));
  pm->Insert(0, 1, std::move(p2));
  pm->Insert(0, 2, std::move(p3));

  auto sender = std::make_shared<SimpleSender>();
  auto cm = std::make_shared<CollectionMap>();
  CollectionView cv;
  cv.collection_id = 0;
  cv.num_partition = 3;
  cm->Insert(cv);
  PartitionTracker pt(id, pm, executor, sender, cm);
  PlanSpec plan;
  plan.plan_id = 0;
  plan.map_collection_id = 0;
  plan.join_collection_id = 0;
  pt.SetPlan(plan);

  pt.RunAllMap([](ShuffleMeta meta, std::shared_ptr<AbstractPartition> p, 
                   std::shared_ptr<AbstractMapProgressTracker> t) {
    auto* part = static_cast<FakePartition<int>*>(p.get());
    int i = 0;
    for (auto elem : part->objs) {
      i += 1;
    }
    VLOG(1) << "map meta: " << meta.DebugString() << " finish " << i;
  });
  // wait for all maps finish
  auto msg = sender->Get();
  for (int i = 0; i < 3; ++ i) {
    for (int j = 0; j < 3; ++ j) {
      JoinMeta join_meta;
      join_meta.part_id = i;
      join_meta.upstream_part_id = j;
      join_meta.func = [i](std::shared_ptr<AbstractPartition> p) {
        int num_objs = static_cast<FakePartition<int>*>(p.get())->objs.size();
        VLOG(1) << "join part: " << i << ", num_objs: "  << num_objs;
      };
      pt.RunJoin(join_meta);
    }
  }
  // wait for all joins finish
  msg = sender->Get();
  EXPECT_EQ(sender->msgs.Size(), 0);
}

}  // namespace
}  // namespace xyz

