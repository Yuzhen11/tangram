#include "gtest/gtest.h"
#include "glog/logging.h"

#include "core/partition/fetcher.hpp"

namespace xyz {
namespace {

struct FakePartitionCache : public AbstractPartitionCache {
  virtual void UpdatePartition(int collection_id, int partition_id, int version, SArrayBinStream bin) override {
  }
};

template <typename T>
class FakePartition : public AbstractPartition {
 public:
  virtual void FromBin(SArrayBinStream& bin) override { bin >> a; }
  virtual void ToBin(SArrayBinStream& bin) override { bin << a; }

  int a;
};

class SimpleSender : public AbstractSender {
 public:
  virtual void Send(Message msg) override {
    msgs.Push(std::move(msg));
  }
  Message Get() {
    Message msg;
    msgs.WaitAndPop(&msg);
    return msg;
  }
  ThreadsafeQueue<Message> msgs;
};

class TestFetcher : public testing::Test {};

TEST_F(TestFetcher, Construct) {
  const int qid = 0;
  auto partition_manager = std::make_shared<PartitionManager>();
  auto partition_cache = std::make_shared<FakePartitionCache>();
  auto sender = std::make_shared<SimpleSender>();
  Fetcher fetcher(qid, partition_manager, partition_cache, sender);
}

TEST_F(TestFetcher, FetchRemote) {
  const int qid = 0;
  auto partition_manager = std::make_shared<PartitionManager>();
  auto partition_cache = std::make_shared<FakePartitionCache>();
  auto sender = std::make_shared<SimpleSender>();
  Fetcher fetcher(qid, partition_manager, partition_cache, sender);
  const int collection_id = 3;
  const int partition_id = 2;
  const int version = 1;
  fetcher.FetchRemote(collection_id, partition_id, version);

  auto msg = sender->Get();
  int a, b, c;
  SArrayBinStream bin;
  bin.FromMsg(msg);
  bin >> a >> b >> c;
  EXPECT_EQ(a, collection_id);
  EXPECT_EQ(b, partition_id);
  EXPECT_EQ(c, version);
}

TEST_F(TestFetcher, FetchLocal) {
  const int qid = 0;
  auto partition_manager = std::make_shared<PartitionManager>();
  auto partition_cache = std::make_shared<FakePartitionCache>();
  auto sender = std::make_shared<SimpleSender>();
  auto part = std::make_shared<FakePartition<int>>();
  const int data = 10;
  part->a = data;
  const int collection_id = 3;
  const int partition_id = 2;
  const int version = 0;
  partition_manager->Insert(collection_id, partition_id, std::move(part));
  Fetcher fetcher(qid, partition_manager, partition_cache, sender);
  SArrayBinStream bin;
  bin << collection_id << partition_id << version;
  Message msg = bin.ToMsg();
  fetcher.FetchLocal(msg);

  auto reply_msg = sender->Get();
  int a, b, c;
  SArrayBinStream reply_bin;
  reply_bin.FromMsg(reply_msg);
  reply_bin >> a >> b >> c;
  EXPECT_EQ(a, collection_id);
  EXPECT_EQ(b, partition_id);
  EXPECT_EQ(c, version);
  FakePartition<int> new_part;
  new_part.FromBin(reply_bin);
  EXPECT_EQ(new_part.a, data);
}

TEST_F(TestFetcher, FetchReply) {
  const int qid = 0;
  auto partition_manager = std::make_shared<PartitionManager>();
  auto partition_cache = std::make_shared<FakePartitionCache>();
  auto sender = std::make_shared<SimpleSender>();
  Fetcher fetcher(qid, partition_manager, partition_cache, sender);
  const int collection_id = 3;
  const int partition_id = 2;
  const int version = 0;
  SArrayBinStream bin;
  bin << collection_id << partition_id << version;
  Message msg = bin.ToMsg();
  fetcher.FetchReply(msg);
}

}  // namespace
}  // namespace xyz

