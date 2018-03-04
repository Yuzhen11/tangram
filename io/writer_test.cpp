#include "glog/logging.h"
#include "gtest/gtest.h"

#include "io/hdfs_writer.hpp"
#include "io/writer.hpp"

#include "base/threadsafe_queue.hpp"
#include "core/partition/seq_partition.hpp"

namespace xyz {
namespace {

class TestWriter : public testing::Test {};

struct ObjT {
  using KeyT = int;
  using ValT = int;
  int key;
  int val;
  KeyT Key() const { return key; }
};

TEST_F(TestWriter, Create) {
  const int qid = 0;
  auto executor = std::make_shared<Executor>(4);
  auto partition_manager = std::make_shared<PartitionManager>();
  Writer writer(qid, executor, partition_manager,
                []() { return std::make_shared<HdfsWriter>("proj10", 9000); });
}

TEST_F(TestWriter, Write) {
  const int qid = 0;
  auto executor = std::make_shared<Executor>(4);
  auto part = std::make_shared<SeqPartition<ObjT>>();
  part->Add(ObjT{1, 2});
  auto partition_manager = std::make_shared<PartitionManager>();
  partition_manager->Insert(0, 0, std::move(part));
  CHECK(partition_manager->Has(0, 0));

  Writer writer(qid, executor, partition_manager,
                []() { return std::make_shared<HdfsWriter>("proj10", 9000); });

  ThreadsafeQueue<SArrayBinStream> q;
  writer.Write(0, 0, "/tmp/tmp/d.txt",
               [&q](SArrayBinStream bin) { q.Push(bin); });
  SArrayBinStream recv_bin;
  q.WaitAndPop(&recv_bin);
}

} // namespace
} // namespace xyz
