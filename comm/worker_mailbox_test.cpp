#include "glog/logging.h"
#include "gtest/gtest.h"

#include "comm/scheduler_mailbox.hpp"
#include "comm/worker_mailbox.hpp"

namespace xyz {
namespace {

class TestWorkerMailbox : public testing::Test {};

TEST_F(TestWorkerMailbox, Construct) {
  Node node{0, "localhost", 32145, false};
  WorkerMailbox mailbox(node, 5);
}

TEST_F(TestWorkerMailbox, BindAndConnect) {
  Node node{0, "localhost", 32145, false};
  WorkerMailbox mailbox(node, 5);
  mailbox.BindAndConnect();
  mailbox.CloseSockets();
}

} // namespace
} // namespace xyz
