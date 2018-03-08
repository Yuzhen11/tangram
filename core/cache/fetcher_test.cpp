#include "gtest/gtest.h"
#include "glog/logging.h"

#include "core/cache/fetcher.hpp"

#include "comm/simple_sender.hpp"
#include "core/index/key_to_part_mappers.hpp"

namespace xyz {
namespace {

class TestFetcher : public testing::Test {};

TEST_F(TestFetcher, Construct) {
  const int qid = 0;
  std::map<int, std::function<
      SArrayBinStream(SArrayBinStream& bin, std::shared_ptr<AbstractPartition>)>> func;
  auto partition_manager = std::make_shared<PartitionManager>();
  auto collection_map = std::make_shared<CollectionMap>();
  auto sender = std::make_shared<SimpleSender>();
  Fetcher fetcher(qid, partition_manager, func, collection_map, sender);
}

}  // namespace
}  // namespace xyz

