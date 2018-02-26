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

std::unordered_map<int, CollectionView> GetCollectionMap() {
  CollectionView c1{1, 10}; // collection_id, num_partition
  CollectionView c2{2, 10};
  std::unordered_map<int, CollectionView> map;
  map.insert({c1.collection_id, c1});
  map.insert({c2.collection_id, c2});
  return map;
}

AssignedBlock GetAssignedBlock() {
  AssignedBlock block;
  block.url = "file";
  block.offset = 0;
  block.id = 0;
  block.collection_id = 0;
  return block;
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
  worker.SetProgram(program);
  worker.RegisterProgram();

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

TEST_F(TestWorker, RunMap) {
  // TODO
}

TEST_F(TestWorker, InitWorkers) {
  // worker
  const int qid = 0;
  EngineElem engine_elem = GetEngineElem();
  auto reader = std::make_shared<FakeReader>();
  Worker worker(qid, engine_elem, reader);
  auto* q = worker.GetWorkQueue();

  // send request
  {
    SArrayBinStream bin;
    std::unordered_map<int, CollectionView> collection_map_ = GetCollectionMap();
    bin << collection_map_;
    SArrayBinStream ctrl_bin;
    ctrl_bin << ScheduleFlag::kInitWorkers;

    Message msg;
    msg.meta.recver = 0;
    msg.meta.flag = Flag::kOthers;
    msg.AddData(ctrl_bin.ToSArray());
    msg.AddData(bin.ToSArray());
    q->Push(msg);
  }

  auto* sender = static_cast<SimpleSender*>(engine_elem.sender.get());

  {
    auto msg = sender->Get();
    // TODO: Check msg.meta
    ASSERT_EQ(msg.data.size(), 2);
    SArrayBinStream ctrl_bin;
    ctrl_bin.FromSArray(msg.data[0]);
    ScheduleFlag flag;
    ctrl_bin >> flag;
    EXPECT_EQ(flag, ScheduleFlag::kInitWorkersReply);
  }
  ASSERT_EQ(sender->msgs.Size(), 0);
}

TEST_F(TestWorker, LoadBlock) {
  // worker
  const int qid = 0;
  EngineElem engine_elem = GetEngineElem();
  auto reader = std::make_shared<FakeReader>();
  Worker worker(qid, engine_elem, reader);
  auto* q = worker.GetWorkQueue();

  // send request
  {
    SArrayBinStream ctrl_bin, bin;
    ScheduleFlag flag = ScheduleFlag::kLoadBlock;
    ctrl_bin << flag;
    AssignedBlock assigned_block = GetAssignedBlock();
    bin << assigned_block;
    Message msg;
    msg.AddData(ctrl_bin.ToSArray());
    msg.AddData(bin.ToSArray());
    q->Push(msg);
  }

  auto* sender = static_cast<SimpleSender*>(engine_elem.sender.get());

  {
    auto msg = sender->Get();
    EXPECT_EQ(msg.meta.sender, qid);
    EXPECT_EQ(msg.meta.recver, 0);
    ASSERT_EQ(msg.data.size(), 2);
    SArrayBinStream ctrl_bin;
    ctrl_bin.FromSArray(msg.data[0]);
    ScheduleFlag flag;
    ctrl_bin >> flag;
    EXPECT_EQ(flag, ScheduleFlag::kFinishBlock);
  }
  ASSERT_EQ(sender->msgs.Size(), 0);
}

TEST_F(TestWorker, Wait) {
  // worker
  const int qid = 0;
  EngineElem engine_elem = GetEngineElem();
  auto reader = std::make_shared<FakeReader>();
  Worker worker(qid, engine_elem, reader);
  auto* q = worker.GetWorkQueue();

  std::thread th([=]() {
    SArrayBinStream ctrl_bin, bin;
    ScheduleFlag flag = ScheduleFlag::kExit;
    ctrl_bin << flag;
    Message msg;
    msg.AddData(ctrl_bin.ToSArray());
    msg.AddData(bin.ToSArray());
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    q->Push(msg);
  });

  worker.Wait();
  VLOG(1) << "Wait end.";
  th.join();
}

}  // namespace
}  // namespace xyz

