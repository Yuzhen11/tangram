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
// KeyToPartMappers,
// BinToPartMappers
// IndexedSeqPartition.
class TestPartitionCache : public testing::Test {};

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

TEST_F(TestPartitionCache, Construct) {
  auto partition_manager = std::make_shared<PartitionManager>();
  auto mappers = std::make_shared<KeyToPartMappers>();
  auto bin_to_part_mappers = std::make_shared<BinToPartMappers>();
  auto sender = std::make_shared<SimpleSender>();
  PartitionCache cache(partition_manager, mappers, bin_to_part_mappers, sender);
}

TEST_F(TestPartitionCache, Get) {
  auto partition_manager = std::make_shared<PartitionManager>();
  auto mappers = std::make_shared<KeyToPartMappers>();
  auto bin_to_part_mappers = std::make_shared<BinToPartMappers>();
  auto sender = std::make_shared<SimpleSender>();
  PartitionCache cache(partition_manager, mappers, bin_to_part_mappers, sender);
  const int collection_id = 0;
  const int part_id = 0;
  IndexedSeqPartition<ObjT> part;
  // key to part mapper
  auto mapper = std::make_shared<HashKeyToPartMapper<int>>(1);
  mappers->Add(collection_id, mapper);
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
    ObjT obj = cache.Get<ObjT>(collection_id, key, version);
    EXPECT_EQ(obj.key, 2);
    EXPECT_EQ(obj.val, 3);
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

