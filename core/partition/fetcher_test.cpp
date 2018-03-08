#include "gtest/gtest.h"
#include "glog/logging.h"

#include "core/partition/fetcher.hpp"
#include "core/partition/indexed_seq_partition.hpp"
#include "core/index/hash_key_to_part_mapper.hpp"
#include "core/index/key_to_part_mappers.hpp"
#include "comm/simple_sender.hpp"

namespace xyz {
namespace {

struct FakePartitionCache : public AbstractPartitionCache {
  virtual void UpdatePartition(int collection_id, int partition_id, int version, SArrayBinStream bin) override {
  }
  virtual std::shared_ptr<VersionedPartition> GetPartition(int collection_id, int partition_id, int version) override {}
};

template <typename T>
class FakePartition : public AbstractPartition {
 public:
  virtual void FromBin(SArrayBinStream& bin) override { bin >> a; }
  virtual void ToBin(SArrayBinStream& bin) override { bin << a; }
  virtual size_t GetSize() const override { return 0; }
  int a;
};

struct ObjT {
  using KeyT = int;
  using ValT = int;
  ObjT() = default;
  ObjT(KeyT key) : a(key), b(0) {}
  KeyT Key() const { return a; }
  int a;
  int b;
  friend SArrayBinStream& operator<<(xyz::SArrayBinStream& stream, const ObjT& obj) {
    stream << obj.b;
    return stream;
  }
  friend SArrayBinStream& operator>>(xyz::SArrayBinStream& stream, ObjT& obj) {
    stream >> obj.b;
    return stream;
  }

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
  bin.FromSArray(msg.data[1]);
  bin >> a >> b >> c;
  EXPECT_EQ(a, collection_id);
  EXPECT_EQ(b, partition_id);
  EXPECT_EQ(c, version);
}

TEST_F(TestFetcher, FetchRemoteObj) {
  const int qid = 0;
  auto partition_manager = std::make_shared<PartitionManager>();
  auto partition_cache = std::make_shared<FakePartitionCache>();
  auto sender = std::make_shared<SimpleSender>();
  auto key_part_mappers = std::make_shared<HashKeyToPartMapper<ObjT::KeyT>>(1);
  auto mappers = std::make_shared<KeyToPartMappers>();
  Fetcher fetcher(qid, partition_manager, partition_cache, sender);
  fetcher.setMappers(mappers);
  const int app_thread_id = 1;
  const int collection_id = 3;
  const int partition_id = 2;
  const int version = 1;
  std::vector<ObjT::KeyT> keys = {0, 1, 4};
  fetcher.FetchRemoteObj<ObjT>(app_thread_id, collection_id, keys, version);

  auto msg = sender->Get();
  int a, b, c;
  SArrayBinStream bin;
  bin.FromSArray(msg.data[1]);
  bin >> a >> b >> c;
  EXPECT_EQ(a, app_thread_id);
  EXPECT_EQ(b, collection_id);
  EXPECT_EQ(c, partition_id);
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
  SArrayBinStream bin, ctrl_bin;
  bin << collection_id << partition_id << version;
  Fetcher::Ctrl ctrl = Fetcher::Ctrl::kFetch;
  ctrl_bin << ctrl;
  Message msg;
  msg.AddData(ctrl_bin.ToSArray());
  msg.AddData(bin.ToSArray());
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

TEST_F(TestFetcher, FetchLocalObj) {
  const int qid = 0;
  auto partition_manager = std::make_shared<PartitionManager>();
  auto partition_cache = std::make_shared<FakePartitionCache>();
  auto sender = std::make_shared<SimpleSender>();
  auto part = std::make_shared<IndexedSeqPartition<ObjT>>();
  auto mapper = std::make_shared<KeyToPartMappers>();
  std::function<SArrayBinStream(SArrayBinStream, Fetcher*)> func = [](SArrayBinStream bin, Fetcher* fetcher) { 
      SArrayBinStream new_bin;
      int app_thread_id, collection_id,partition_id,version;
      std::vector<typename ObjT::KeyT> keys;
      bin >> app_thread_id >> collection_id >> partition_id >> keys >> version;
      // TODO: what if the required version is not there ?
      CHECK(fetcher->getPartMng()->Has(collection_id, partition_id, version));
      auto part = fetcher->getPartMng()->Get(collection_id, partition_id, version);
      auto* indexed_part = dynamic_cast<Indexable<ObjT>*>(part->partition.get());
      std::vector<ObjT> objs;
      for (auto key : keys) {
        objs.push_back(indexed_part->Get(key));
      }
      new_bin << app_thread_id << collection_id << partition_id << objs;
      return new_bin;
  };

  part->FindOrCreate(0);
  part->FindOrCreate(1);
  part->FindOrCreate(4);
  const int collection_id = 3;
  const int partition_id = 2;
  const int version = 0;
  const int app_thread_id = 1;
  std::vector<int> keys = {0,1};
  partition_manager->Insert(collection_id, partition_id, std::move(part));
  Fetcher fetcher(qid, partition_manager, partition_cache, sender);
  fetcher.setMappers(mapper);
  fetcher.setFunc(func);
  SArrayBinStream bin, ctrl_bin;
  bin << app_thread_id << collection_id << partition_id << keys << version;
  Fetcher::Ctrl ctrl = Fetcher::Ctrl::kFetchObj;
  ctrl_bin << ctrl;
  Message msg;
  msg.AddData(ctrl_bin.ToSArray());
  msg.AddData(bin.ToSArray());
  fetcher.FetchLocalObj(msg);

  auto reply_msg = sender->Get();
  int a, b, c;
  std::vector<ObjT> objs;
  SArrayBinStream reply_bin;
  reply_bin.FromMsg(reply_msg);
  reply_bin >> a >> b >> c >> objs;
  EXPECT_EQ(a, app_thread_id);
  EXPECT_EQ(b, collection_id);
  EXPECT_EQ(c, partition_id);
  EXPECT_EQ(objs.size(), 3);
  EXPECT_EQ(objs[0].b, 0);
  EXPECT_EQ(objs[1].b, 1);
  EXPECT_EQ(objs[4].b, 4);
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
  SArrayBinStream bin, ctrl_bin;
  bin << collection_id << partition_id << version;
  Fetcher::Ctrl ctrl = Fetcher::Ctrl::kFetch;
  ctrl_bin << ctrl;
  Message msg;
  msg.AddData(ctrl_bin.ToSArray());
  msg.AddData(bin.ToSArray());
  fetcher.FetchReply(msg);
}

TEST_F(TestFetcher, FetchObjReply) {
  const int qid = 0;
  auto partition_manager = std::make_shared<PartitionManager>();
  auto partition_cache = std::make_shared<FakePartitionCache>();
  auto sender = std::make_shared<SimpleSender>();
  Fetcher fetcher(qid, partition_manager, partition_cache, sender);
  const int collection_id = 3;
  const int partition_id = 2;
  const int app_thread_id = 1;
  SArrayBinStream bin, ctrl_bin;
  bin << app_thread_id << collection_id << partition_id;
  Fetcher::Ctrl ctrl = Fetcher::Ctrl::kFetchObj;
  ctrl_bin << ctrl;
  Message msg;
  msg.AddData(ctrl_bin.ToSArray());
  msg.AddData(bin.ToSArray());
  fetcher.FetchObjReply(msg);
}



}  // namespace
}  // namespace xyz

