#include "gtest/gtest.h"
#include "glog/logging.h"

#include "io/loader.hpp"

#include "io/meta.hpp"
#include "core/partition/seq_partition.hpp"

#include "io/fake_reader.hpp"
#include "base/threadsafe_queue.hpp"

#include <thread>

namespace xyz {
namespace {

class TestLoader : public testing::Test {};

TEST_F(TestLoader, Create) {
  const int qid = 0;
  auto executor = std::make_shared<Executor>(4);
  auto partition_manager = std::make_shared<PartitionManager>();
  std::string namenode = "fake_namenode";
  int port = 9000;
  Node node;
  node.id = 2;
  node.hostname = "proj10";
  auto reader_getter = []() { return std::make_shared<FakeReader>(); };
  Loader loader(qid, executor, partition_manager, 
          namenode, port, node, reader_getter);
}

TEST_F(TestLoader, Load) {
  const int qid = 0;
  auto executor = std::make_shared<Executor>(4);
  auto partition_manager = std::make_shared<PartitionManager>();
  std::string namenode = "fake_namenode";
  int port = 9000;
  Node node;
  node.id = 2;
  node.hostname = "proj10";
  auto reader_getter = []() { return std::make_shared<FakeReader>(); };
  Loader loader(qid, executor, partition_manager, 
          namenode, port, node, reader_getter);

  const int block_id = 23;
  const int collection_id = 12; 
  const size_t offset = 2342342;
  const std::string url = "kdd";
  AssignedBlock block{url, offset, block_id, collection_id};
  ThreadsafeQueue<SArrayBinStream> q;
  loader.Load(block, [&q](SArrayBinStream bin) {
    q.Push(bin);
  });
  SArrayBinStream recv_bin;
  q.WaitAndPop(&recv_bin);
  FinishedBlock finished_block;
  recv_bin >> finished_block;
  LOG(INFO) << "finsih: " << finished_block.DebugString();
  EXPECT_EQ(finished_block.block_id, block_id);
  EXPECT_EQ(finished_block.node_id, node.id);
  EXPECT_EQ(finished_block.qid, qid);
  EXPECT_EQ(finished_block.hostname, node.hostname);
  EXPECT_EQ(finished_block.collection_id, collection_id);

  auto part = partition_manager->Get(collection_id, block_id)->partition;
  auto* p = static_cast<SeqPartition<std::string>*>(part.get());
  auto v = p->GetStorage();
  ASSERT_EQ(v.size(), 3);
  EXPECT_EQ(v[0], "a");
  EXPECT_EQ(v[1], "b");
  EXPECT_EQ(v[2], "c");
}

}  // namespace
}  // namespace xyz

