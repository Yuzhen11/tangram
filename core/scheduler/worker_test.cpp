#include "gtest/gtest.h"
#include "glog/logging.h"

#include "worker.hpp"
#include "comm/simple_sender.hpp"

namespace xyz {
namespace {

class TestWorker : public testing::Test {};

struct FakeReader : public AbstractReader {
 public:
  virtual std::vector<std::string> Read(std::string namenode, int port, 
          std::string url, size_t offset) override {
    VLOG(1) << "namenode: " << namenode << ", port: " << port
        << ", url: " << url << ", offset: " << offset;
    return {"a", "b", "c"};
  }
};


EngineElem GetEngineElem() {
  const int num_threads = 1;
  const std::string namenode = "fake_namenode";
  const int port = 1000;
  EngineElem engine_elem;
  engine_elem.executor = std::make_shared<Executor>(num_threads);
  engine_elem.partition_manager = std::make_shared<PartitionManager>();
  engine_elem.function_store = std::make_shared<FunctionStore>();
  engine_elem.intermediate_store = std::make_shared<SimpleIntermediateStore>();
  engine_elem.partition_tracker = std::make_shared<PartitionTracker>(
          engine_elem.partition_manager, engine_elem.executor);
  engine_elem.namenode = namenode;
  engine_elem.port = port;
  Node node;
  engine_elem.node = node;
  engine_elem.sender = std::make_shared<SimpleSender>();
  return engine_elem;
}

TEST_F(TestWorker, Create) {
  const int qid = 0;
  EngineElem engine_elem = GetEngineElem();
  auto reader = std::make_shared<FakeReader>();
  Worker worker(qid, engine_elem, reader);
}

TEST_F(TestWorker, RegisterProgram) {
  // program
  ProgramContext program;
  const int pid = 0;
  const int mid = 1;
  const int jid = 2;
  const int wid = -1;
  const int num_parts = 10;
  PlanSpec plan{pid, mid, jid, wid};
  CollectionView c1{mid, num_parts};
  CollectionView c2{jid, num_parts};
  program.plans.push_back(plan);
  program.collections.push_back(c1);
  program.collections.push_back(c2);

  // worker
  const int qid = 0;
  EngineElem engine_elem = GetEngineElem();
  auto reader = std::make_shared<FakeReader>();
  Worker worker(qid, engine_elem, reader);

  worker.RegisterProgram(program);
  Message msg = static_cast<SimpleSender*>(engine_elem.sender.get())->Get();
  ASSERT_EQ(msg.data.size(), 2);
  ProgramContext p;
  SArrayBinStream bin;
  bin.FromSArray(msg.data[1]);
  bin >> p;
  VLOG(3) << p.DebugString();
  ASSERT_EQ(p.plans.size(), 1); 
  EXPECT_EQ(p.plans[0].plan_id, pid);
  EXPECT_EQ(p.plans[0].map_collection_id, mid);
  EXPECT_EQ(p.plans[0].join_collection_id, jid);
  EXPECT_EQ(p.plans[0].with_collection_id, wid);
  ASSERT_EQ(p.collections.size(), 2); 
  EXPECT_EQ(p.collections[0].collection_id, mid);
  EXPECT_EQ(p.collections[0].num_partition, num_parts);
  EXPECT_EQ(p.collections[1].collection_id, jid);
  EXPECT_EQ(p.collections[1].num_partition, num_parts);
  EXPECT_EQ(p.plans[0].map_collection_id, mid);
}

}  // namespace
}  // namespace xyz

