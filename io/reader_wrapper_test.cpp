#include "glog/logging.h"
#include "gtest/gtest.h"

#include "io/reader_wrapper.hpp"
#include "io/fake_reader.hpp"

#include "base/threadsafe_queue.hpp"
#include "core/partition/seq_partition.hpp"

namespace xyz {
namespace {

class TestReaderWrapper : public testing::Test {};

TEST_F(TestReaderWrapper, Create) {
  const int qid = 0;
  auto executor = std::make_shared<Executor>(4);
  auto partition_manager = std::make_shared<PartitionManager>();
  ReaderWrapper reader(qid, executor, partition_manager,
                []() { return std::make_shared<FakeReader>(); });
}
} // namespace
} // namespace xyz
