#include "gtest/gtest.h"
#include "glog/logging.h"

#include "io/loader.hpp"

#include "io/meta.hpp"
#include "comm/simple_sender.hpp"
#include "core/partition/seq_partition.hpp"

#include <thread>

namespace xyz {
namespace {

class TestLoader : public testing::Test {};

struct FakeReader : public AbstractReader {
 public:
  virtual std::vector<std::string> Read(std::string namenode, int port, 
          std::string url, size_t offset) override {
    VLOG(1) << "namenode: " << namenode << ", port: " << port
        << ", url: " << url << ", offset: " << offset;
    return {"a", "b", "c"};
  }
};

TEST_F(TestLoader, Create) {
  const int qid = 0;
  auto sender = std::make_shared<SimpleSender>();
  auto reader = std::make_shared<FakeReader>();
  auto executor = std::make_shared<Executor>(4);
  auto partition_manager = std::make_shared<PartitionManager>();
  std::string namenode = "fake_namenode";
  int port = 9000;
  Node node;
  node.id = 2;
  node.hostname = "proj10";
  Loader loader(qid, sender, reader, executor, partition_manager, namenode, port, node);
}

TEST_F(TestLoader, Load) {
  const int qid = 0;
  auto sender = std::make_shared<SimpleSender>();
  auto reader = std::make_shared<FakeReader>();
  auto executor = std::make_shared<Executor>(4);
  auto partition_manager = std::make_shared<PartitionManager>();
  std::string namenode = "fake_namenode";
  int port = 9000;
  Node node;
  node.id = 2;
  node.hostname = "proj10";
  Loader loader(qid, sender, reader, executor, partition_manager, namenode, port, node);

  const int block_id = 23;
  const int collection_id = 12; 
  const size_t offset = 2342342;
  const std::string url = "kdd";
  AssignedBlock block{url, offset, block_id, collection_id};
  loader.Load(block);

  auto recv_msg = sender->Get();
  SArrayBinStream recv_bin;
  CHECK_EQ(recv_msg.data.size(), 2);
  recv_bin.FromSArray(recv_msg.data[1]);
  FinishedBlock finished_block;
  recv_bin >> finished_block;
  LOG(INFO) << "finsih: " << finished_block.DebugString();
  EXPECT_EQ(finished_block.block_id, block_id);
  EXPECT_EQ(finished_block.node_id, node.id);
  EXPECT_EQ(finished_block.qid, qid);
  EXPECT_EQ(finished_block.hostname, node.hostname);
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

