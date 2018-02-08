#include "gtest/gtest.h"
#include "glog/logging.h"

#include "core/index/part_to_node_manager.hpp"

namespace xyz {
namespace {

class TestPartToNodeManager : public testing::Test {};

class SimpleSender : public AbstractSender {
 public:
  virtual void Send(Message msg) override {
    msgs.Push(std::move(msg));
  }
  Message Get() {
    Message msg;
    msgs.WaitAndPop(&msg);
    return msg;
  }
  ThreadsafeQueue<Message> msgs;
};

TEST_F(TestPartToNodeManager, Construct) {
  const int qid = 0;
  auto sender = std::make_shared<SimpleSender>();
  PartToNodeManager manager(qid, sender);
}

}  // namespace
}  // namespace xyz

