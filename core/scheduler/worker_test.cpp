#include "glog/logging.h"
#include "gtest/gtest.h"

#include "comm/simple_sender.hpp"
#include "io/fake_block_reader.hpp"
#include "io/fake_reader.hpp"
#include "io/fake_writer.hpp"
#include "io/meta.hpp"
#include "core/scheduler/worker.hpp"

#include "core/partition/seq_partition.hpp"

namespace xyz {
namespace {

class TestWorker : public testing::Test {};

class TestWriter : public testing::Test {};

struct ObjT {
  using KeyT = int;
  using ValT = int;
  int key;
  int val;
  KeyT Key() const { return key; }
};

EngineElem GetEngineElem() {
  const int num_threads = 1;
  const std::string namenode = "fake_namenode";
  const int port = 1000;
  Node node;
  node.id = 0;
  EngineElem engine_elem;
  engine_elem.node = node;
  engine_elem.executor = std::make_shared<Executor>(num_threads);
  engine_elem.partition_manager = std::make_shared<PartitionManager>();
  engine_elem.collection_map = std::make_shared<CollectionMap>();
  engine_elem.function_store = std::make_shared<FunctionStore>();
  engine_elem.intermediate_store = std::make_shared<SimpleIntermediateStore>();
  engine_elem.sender = std::make_shared<SimpleSender>();
  engine_elem.namenode = namenode;
  engine_elem.port = port;
  engine_elem.num_local_threads = 1;
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
  auto io_wrapper = std::make_shared<IOWrapper>(
      []() { return std::make_shared<FakeReader>(); },
      []() { return std::make_shared<FakeWriter>(); });
  Worker worker(qid, engine_elem, io_wrapper,
          []() { return std::make_shared<FakeBlockReader>(); }
          );
}

TEST_F(TestWorker, Wait) {
  // worker
  const int qid = 0;
  EngineElem engine_elem = GetEngineElem();
  auto io_wrapper = std::make_shared<IOWrapper>(
      []() { return std::make_shared<FakeReader>(); },
      []() { return std::make_shared<FakeWriter>(); });
  Worker worker(qid, engine_elem, io_wrapper,
          []() { return std::make_shared<FakeBlockReader>(); }
          );
  auto *q = worker.GetWorkQueue();

  std::thread th([=]() {
    SArrayBinStream ctrl_bin, bin;
    ScheduleFlag flag = ScheduleFlag::kExit;
    ctrl_bin << flag;
    Message msg;
    msg.meta.flag = Flag::kOthers;
    msg.AddData(ctrl_bin.ToSArray());
    msg.AddData(bin.ToSArray());
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    q->Push(msg);
  });

  worker.Wait();
  VLOG(1) << "Wait end.";
  th.join();
}

} // namespace
} // namespace xyz
