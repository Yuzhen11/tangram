#include "glog/logging.h"
#include "gtest/gtest.h"

#include "comm/simple_sender.hpp"
#include "io/abstract_browser.hpp"
#include "io/assigner.hpp"
#include "io/meta.hpp"

namespace xyz {
namespace {

class TestAssigner : public testing::Test {};

struct FakeBrowser : public AbstractBrowser {
  virtual std::vector<BlockInfo> Browse(std::string url) override {
    std::vector<BlockInfo> v{{"file0", 0, "node0"},   {"file0", 0, "node1"},
                             {"file0", 0, "node2"},   {"file0", 100, "node2"},
                             {"file0", 100, "node3"}, {"file0", 100, "node0"},
                             {"file1", 0, "node3"},   {"file1", 0, "node0"},
                             {"file1", 0, "node1"}};
    return v;
  }
};

TEST_F(TestAssigner, Create) {
  auto sender = std::make_shared<SimpleSender>();
  auto browser = std::make_shared<FakeBrowser>();
  Assigner assigner(sender, browser);
}

/*
TEST_F(TestAssigner, InitBlocks) {
  auto sender = std::make_shared<SimpleSender>();
  auto browser = std::make_shared<FakeBrowser>();
  Assigner assigner(sender, browser);
  assigner.InitBlocks("dummy");
  VLOG(1) << "locality_map_: \n" << assigner.DebugStringLocalityMap();
  VLOG(1) << "blocks_: \n" << assigner.DebugStringBlocks();
}

TEST_F(TestAssigner, Load) {
  auto sender = std::make_shared<SimpleSender>();
  auto browser = std::make_shared<FakeBrowser>();
  Assigner assigner(sender, browser);
  int collection_id = 0;
  assigner.Load(collection_id, "dummy", {{"node0", 0}, {"node1", 1}}, {1,1});
  EXPECT_EQ(assigner.Done(), false);
  FinishedBlock b0{0, 0, 0, "node0", collection_id};
  EXPECT_EQ(assigner.FinishBlock(b0), false);
  FinishedBlock b1{1, 1, 0, "node1", collection_id};
  EXPECT_EQ(assigner.FinishBlock(b1), false);
  FinishedBlock b2{2, 0, 0, "node0", collection_id};
  EXPECT_EQ(assigner.FinishBlock(b2), true);
}
*/

} // namespace
} // namespace xyz
