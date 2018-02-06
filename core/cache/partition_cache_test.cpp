#include "gtest/gtest.h"
#include "glog/logging.h"

#include <thread>

#include "base/threadsafe_queue.hpp"
#include "core/cache/partition_cache.hpp"
#include "core/partition/indexed_seq_partition.hpp"
#include "core/index/hash_key_to_part_mapper.hpp"

namespace xyz {
namespace {

struct ObjT {
  using KeyT = int;
  using ValT = int;
  ObjT() = default;
  ObjT(KeyT _key):key(_key) {}
  ObjT(KeyT _key, ValT _val):key(_key), val(_val) {}
  int key;
  int val;
  KeyT Key() const { return key; }
};

// This test also depends on 
// PartitionManager,
// BinToPartMappers
// IndexedSeqPartition.
class TestPartitionCache : public testing::Test {};

class SimpleFetcher : public AbstractFetcher {
 public:
  virtual void FetchRemote(int collection_id, int partition_id, int version) {
    SArrayBinStream bin;
    bin << collection_id << partition_id << version;
    Message msg = bin.ToMsg();
    msgs.Push(std::move(msg));
  }
  Message Get() {
    Message msg;
    msgs.WaitAndPop(&msg);
    return msg;
  }
  ThreadsafeQueue<Message> msgs;
};

TEST_F(TestPartitionCache, Construct) {
  auto partition_manager = std::make_shared<PartitionManager>();
  auto bin_to_part_mappers = std::make_shared<BinToPartMappers>();
  auto sender = std::make_shared<SimpleFetcher>();
  PartitionCache cache(partition_manager, bin_to_part_mappers, sender);
}

TEST_F(TestPartitionCache, Get) {
  auto partition_manager = std::make_shared<PartitionManager>();
  auto bin_to_part_mappers = std::make_shared<BinToPartMappers>();
  auto sender = std::make_shared<SimpleFetcher>();
  PartitionCache cache(partition_manager, bin_to_part_mappers, sender);
  const int collection_id = 0;
  const int part_id = 0;
  IndexedSeqPartition<ObjT> part;
  // bin to part mapper
  auto bin_to_part = [](SArrayBinStream bin) {
    auto part = std::make_shared<IndexedSeqPartition<ObjT>>();
    part->FromBin(bin);
    return part;
  };
  bin_to_part_mappers->Add(collection_id, bin_to_part);
  const int version = 0;
  const int key = 2;
  std::thread th1([&cache]() {
    auto part = cache.GetPartition(collection_id, part_id, version)->partition;
    auto typed_part = static_cast<IndexedSeqPartition<ObjT>*>(part.get());
    EXPECT_EQ(typed_part->Get(2).val, 3);
    EXPECT_EQ(typed_part->Get(1).val, 2);
  });
  std::thread th2([&cache, sender, collection_id, part_id, version]() {
    Message msg = sender->Get();
    SArrayBinStream bin;
    auto part = std::make_shared<IndexedSeqPartition<ObjT>>();
    part->Add({1, 2});
    part->Add({2, 3});
    part->ToBin(bin);
    cache.UpdatePartition(collection_id, part_id, version, bin);
  });
  th1.join();
  th2.join();
}

}  // namespace
}  // namespace xyz

