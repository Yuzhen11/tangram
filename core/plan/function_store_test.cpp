#include "gtest/gtest.h"
#include "glog/logging.h"

#include "core/plan/function_store.hpp"

/*
 * This test depends on SeqPartition, MapOutput, MapOutputManager, and SimpleIntermediateStore.
 */
#include "core/partition/seq_partition.hpp"
#include "core/map_output/map_output.hpp"
#include "core/map_output/map_output_storage.hpp"
#include "core/intermediate/simple_intermediate_store.hpp"

namespace xyz {
namespace {

struct ObjT {
  using KeyT = int;
  using ValT = int;
  KeyT Key() const { return a; }
  int a;
};

class TestFunctionStore: public testing::Test {};

struct FakeMapProgressTracker : public AbstractMapProgressTracker {
  virtual void Report(int) {
  }
};

struct FakeCollectionMap : public AbstractCollectionMap {
  virtual int Lookup(int collection_id, int part_id) override {
    return part_id;
  }
};

TEST_F(TestFunctionStore, GetPartToOutputManager) {
  const int plan_id = 0;
  const int num_part = 1;
  auto partition = std::make_shared<SeqPartition<ObjT>>();
  partition->Add(ObjT{10});
  partition->Add(ObjT{20});
  auto map_output_storage = std::make_shared<MapOutputManager>();
  auto tracker = std::make_shared<FakeMapProgressTracker>();

  auto map = [](std::shared_ptr<AbstractPartition> partition, 
                std::shared_ptr<AbstractMapProgressTracker> tracker) {
    auto* p = static_cast<TypedPartition<ObjT>*>(partition.get());
    auto output = std::make_shared<MapOutput<ObjT::KeyT, int>>();
    for (auto& elem : *p) {
      output->Add({elem.a, 1});
    }
    return output;
  };
  auto collection_map = std::make_shared<FakeCollectionMap>();
  FunctionStore function_store(collection_map);
  function_store.AddPartToOutputManager(plan_id, map);
  auto func = function_store.GetMapPart1(plan_id);
  ASSERT_EQ(map_output_storage->Get(0).size(), 0);
  func(partition, map_output_storage, tracker);
  ASSERT_EQ(map_output_storage->Get(0).size(), 1);
  auto map_output = map_output_storage->Get(0)[0];
  auto vec = static_cast<MapOutput<int,int>*>(map_output.get())->Get();
  ASSERT_EQ(vec.size(), 2);
  EXPECT_EQ(vec[0].first, 10);
  EXPECT_EQ(vec[0].second, 1);
  EXPECT_EQ(vec[1].first, 20);
  EXPECT_EQ(vec[1].second, 1);
}

TEST_F(TestFunctionStore, GetPartToIntermediate) {
  const int plan_id = 0;
  const int num_part = 1;
  auto partition = std::make_shared<SeqPartition<ObjT>>();
  partition->Add(ObjT{10});
  partition->Add(ObjT{20});
  auto intermediate_store = std::make_shared<SimpleIntermediateStore>();
  auto tracker = std::make_shared<FakeMapProgressTracker>();

  auto map = [](std::shared_ptr<AbstractPartition> partition,
                std::shared_ptr<AbstractMapProgressTracker> tracker) {
    auto* p = static_cast<TypedPartition<ObjT>*>(partition.get());
    auto output = std::make_shared<MapOutput<ObjT::KeyT, int>>();
    for (auto& elem : *p) {
      output->Add({elem.a, 1});
    }
    return output;
  };
  auto collection_map = std::make_shared<FakeCollectionMap>();
  FunctionStore function_store(collection_map);
  function_store.AddPartToIntermediate(plan_id, map);
  auto func = function_store.GetMap(plan_id);
  ShuffleMeta meta;
  func(meta, partition, intermediate_store, tracker);
  auto msgs = intermediate_store->Get();
  ASSERT_EQ(msgs.size(), 1);
  auto msg = msgs[0];
  ASSERT_EQ(msg.data.size(), 2);
  SArrayBinStream bin;
  bin.FromSArray(msg.data[1]);
  int k, v;
  bin >> k >> v;
  EXPECT_EQ(k, 10);
  EXPECT_EQ(v, 1);
  bin >> k >> v;
  EXPECT_EQ(k, 20);
  EXPECT_EQ(v, 1);
}

}  // namespace
}  // namespace xyz

