#include "glog/logging.h"
#include "gtest/gtest.h"

#include "io/writer_wrapper.hpp"
#include "io/fake_writer.hpp"

#include "base/threadsafe_queue.hpp"
#include "core/partition/seq_partition.hpp"

namespace xyz {
namespace {

class TestWriterWrapper : public testing::Test {};

struct ObjT {
  using KeyT = int;
  using ValT = int;
  int key;
  int val;
  KeyT Key() const { return key; }
};


TEST_F(TestWriterWrapper, Create) {
  const int qid = 0;
  auto executor = std::make_shared<Executor>(4);
  auto partition_manager = std::make_shared<PartitionManager>();
  WriterWrapper writer(qid, executor, partition_manager,
                []() { return std::make_shared<FakeWriter>(); });
}

TEST_F(TestWriterWrapper, Write) {
  const int qid = 0;
  auto executor = std::make_shared<Executor>(4);
  auto part = std::make_shared<SeqPartition<ObjT>>();
  part->Add(ObjT{1, 2});
  auto partition_manager = std::make_shared<PartitionManager>();
  partition_manager->Insert(0, 0, std::move(part));
  CHECK(partition_manager->Has(0, 0));

  WriterWrapper writer(qid, executor, partition_manager,
                []() { return std::make_shared<FakeWriter>(); });

  ThreadsafeQueue<SArrayBinStream> q;
  writer.Write(0, 0, "/tmp/tmp/d.txt",
               [&q](SArrayBinStream bin) { q.Push(bin); });
  SArrayBinStream recv_bin;
  q.WaitAndPop(&recv_bin);
}

} // namespace
} // namespace xyz
