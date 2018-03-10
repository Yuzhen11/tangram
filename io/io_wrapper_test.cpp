#include "glog/logging.h"
#include "gtest/gtest.h"

#include "io/io_wrapper.hpp"
#include "io/hdfs_reader.hpp"
#include "io/fake_reader.hpp"
#include "io/fake_writer.hpp"

#include "base/threadsafe_queue.hpp"
#include "core/partition/seq_partition.hpp"

namespace xyz {
namespace {

class TestIOWrapper : public testing::Test {};

struct ObjT {
  using KeyT = int;
  using ValT = int;
  int key;
  int val;
  KeyT Key() const { return key; }
};


TEST_F(TestIOWrapper, Create) {
  const int qid = 0;
  auto executor = std::make_shared<Executor>(4);
  auto partition_manager = std::make_shared<PartitionManager>();
  auto function_store = std::make_shared<FunctionStore>();
  IOWrapper io_wrapper(qid, executor, partition_manager, function_store,
                []() { return std::make_shared<FakeReader>(); },
                []() { return std::make_shared<FakeWriter>(); });
}

TEST_F(TestIOWrapper, Write) {
  const int qid = 0;
  auto executor = std::make_shared<Executor>(4);
  auto part = std::make_shared<SeqPartition<ObjT>>();
  part->Add(ObjT{1, 2});
  auto partition_manager = std::make_shared<PartitionManager>();
  auto function_store = std::make_shared<FunctionStore>();
  partition_manager->Insert(0, 0, std::move(part));
  CHECK(partition_manager->Has(0, 0));

  IOWrapper io_wrapper(qid, executor, partition_manager, function_store,
                []() { return std::make_shared<FakeReader>(); },
                []() { return std::make_shared<FakeWriter>(); });

  ThreadsafeQueue<SArrayBinStream> q;
  io_wrapper.Write(0, 0, "/tmp/tmp/d.txt",
               [](std::shared_ptr<AbstractPartition> p, 
                 std::shared_ptr<AbstractWriter> writer, std::string url) { 
                 SArrayBinStream bin;
                 p->ToBin(bin);
                 bool rc = writer->Write(url, bin.GetPtr(), bin.Size());
                 CHECK_EQ(rc, 0);
               },
               [&q](SArrayBinStream bin) { q.Push(bin); }
               );
  SArrayBinStream recv_bin;
  q.WaitAndPop(&recv_bin);
}

TEST_F(TestIOWrapper, Read) {
  const int qid = 0;
  auto executor = std::make_shared<Executor>(4);
  auto partition_manager = std::make_shared<PartitionManager>();
  auto function_store = std::make_shared<FunctionStore>();
  function_store->AddCreatePartFunc(0, []() { return std::make_shared<SeqPartition<ObjT>>(); });

  IOWrapper io_wrapper(qid, executor, partition_manager, function_store,
                []() { return std::make_shared<HdfsReader>("proj10", 9000); },
                []() { return std::make_shared<FakeWriter>(); });
  
  ThreadsafeQueue<SArrayBinStream> q;
  io_wrapper.Read(0, 0, "/tmp/tmp/d.txt",
               [&q](SArrayBinStream bin) { q.Push(bin); }
               );
  SArrayBinStream recv_bin;
  q.WaitAndPop(&recv_bin);
}

} // namespace
} // namespace xyz
