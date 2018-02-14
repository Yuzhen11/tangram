#include "gtest/gtest.h"
#include "glog/logging.h"

#include "core/index/part_to_node_manager.hpp"
#include "comm/simple_sender.hpp"

namespace xyz {
namespace {

class TestPartToNodeManager : public testing::Test {};

TEST_F(TestPartToNodeManager, Construct) {
  const int qid = 0;
  auto sender = std::make_shared<SimpleSender>();
  PartToNodeManager manager(qid, sender);
}

}  // namespace
}  // namespace xyz

