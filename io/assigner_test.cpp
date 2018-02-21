#include "gtest/gtest.h"
#include "glog/logging.h"

#include <thread>
#include <future>

#include "io/assigner.hpp"
#include "io/abstract_browser.hpp"
#include "io/meta.hpp"
#include "comm/simple_sender.hpp"

namespace xyz {
namespace {

class TestAssigner : public testing::Test {};

struct FakeBrowser : public AbstractBrowser {
  virtual std::vector<BlockInfo> Browse(std::string url) override {
    std::vector<BlockInfo> v {
      {"file0", 0, "node0"},
      {"file0", 0, "node1"},
      {"file0", 0, "node2"},
      {"file0", 100, "node2"},
      {"file0", 100, "node3"},
      {"file0", 100, "node0"},
      {"file1", 0, "node3"},
      {"file1", 0, "node0"},
      {"file1", 0, "node1"}
    };
    return v;
  }
};

TEST_F(TestAssigner, Create) {
  const int qid = 0;
  auto sender = std::make_shared<SimpleSender>();
  auto browser = std::make_shared<FakeBrowser>();
  Assigner assigner(qid, sender, browser);
}

TEST_F(TestAssigner, InitBlocks) {
  const int qid = 0;
  auto sender = std::make_shared<SimpleSender>();
  auto browser = std::make_shared<FakeBrowser>();
  Assigner assigner(qid, sender, browser);
  assigner.InitBlocks("dummy");
  VLOG(1) << "locality_map_: \n" << assigner.DebugStringLocalityMap();
  VLOG(1) << "blocks_: \n" << assigner.DebugStringBlocks();
}

TEST_F(TestAssigner, Load) {
  const int qid = 0;
  auto sender = std::make_shared<SimpleSender>();
  auto browser = std::make_shared<FakeBrowser>();
  Assigner assigner(qid, sender, browser);
  auto* q = assigner.GetWorkQueue();
  std::promise<void> promise;
  auto future = promise.get_future();
  std::thread th1([&]() {
    assigner.Load("dummy", {{"node0", 0}, {"node1", 1}}, 1);
    promise.set_value();
    assigner.Wait();
  });
  std::thread th2([&]() {
    future.wait();
    SArrayBinStream ctrl_bin, bin;
    ctrl_bin << int(0);
    FinishedBlock b{0, 0, 0, "node0"};
    bin << b;
    Message msg;
    msg.AddData(ctrl_bin.ToSArray());
    msg.AddData(bin.ToSArray());
    q->Push(msg);

    // The last two messages are for finishing reading the blocks.
    q->Push(msg);
    Message msg2;
    msg2.AddData(ctrl_bin.ToSArray());
    SArrayBinStream bin2;
    FinishedBlock b2{-1, 1, 1, "node1"};
    bin2 << b2;
    msg2.AddData(bin2.ToSArray());
    q->Push(msg2);
  });
  th1.join();
  th2.join();
}

}  // namespace
}  // namespace xyz

