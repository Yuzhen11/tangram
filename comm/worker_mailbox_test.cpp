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

/*
TEST_F(TestWorkerMailbox, StartStop) {
  Node node{0, "localhost", 32145, false};
  std::thread th1([=]() {
    // Scheduler
    SchedulerMailbox mailbox(node, 1);
    mailbox.Start();
    mailbox.Stop();
  });
  std::thread th2([=]() {
    // Worker
    WorkerMailbox mailbox(node, 1);
    mailbox.Start();
    mailbox.Stop();
  });
  th1.join();
  th2.join();
}
*/

} // namespace
} // namespace xyz
