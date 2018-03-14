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
  auto function_store = std::make_shared<FunctionStore>();
  auto collection_map = std::make_shared<CollectionMap>();
  auto sender = std::make_shared<SimpleSender>();
  Fetcher fetcher(qid, function_store, collection_map, sender);
}

}  // namespace
}  // namespace xyz

