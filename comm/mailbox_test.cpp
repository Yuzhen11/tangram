#include "glog/logging.h"
#include "gtest/gtest.h"

#include "comm/mailbox.hpp"

namespace xyz {
namespace {

class TestMailbox : public testing::Test {};

TEST_F(TestMailbox, Construct) {
  Node node{0, "localhost", 32145, false};
  Mailbox mailbox(true, node, 5);
}

TEST_F(TestMailbox, BindAndConnect) {
  Node node{0, "localhost", 32145, false};
  Mailbox mailbox(true, node, 5);
  mailbox.BindAndConnect();
  mailbox.CloseSockets();
}

TEST_F(TestMailbox, SendAndRecv) {
  Node node{0, "localhost", 32145, false};
  Mailbox mailbox(true, node, 5);
  mailbox.BindAndConnect();

  Message msg;
  msg.meta.sender = Node::kEmpty;
  msg.meta.recver = 0;
  msg.meta.flag = Flag::kOthers;
  third_party::SArray<int> keys{1};
  third_party::SArray<float> vals{0.4};
  msg.AddData(keys);
  msg.AddData(vals);

  mailbox.Send(msg);
  VLOG(1) << "Finished sending";
  Message recv_msg;
  mailbox.Recv(&recv_msg);
  VLOG(1) << "Finished reciving";
  EXPECT_EQ(recv_msg.meta.sender, msg.meta.sender);
  EXPECT_EQ(recv_msg.meta.recver, msg.meta.recver);
  EXPECT_EQ(recv_msg.meta.flag, msg.meta.flag);
  EXPECT_EQ(recv_msg.data.size(), 2);
  third_party::SArray<int> recv_keys;
  recv_keys = recv_msg.data[0];
  third_party::SArray<float> recv_vals;
  recv_vals = recv_msg.data[1];
  EXPECT_EQ(recv_keys[0], keys[0]);
  EXPECT_EQ(recv_vals[0], vals[0]);
  
  mailbox.CloseSockets();
}

TEST_F(TestMailbox, StartStop) {
  Node node{0, "localhost", 32145, false};
  std::thread th1([=]() {
    // Scheduler
    Mailbox mailbox(true, node, 1);
    mailbox.Start();
    mailbox.Stop();
  });
  std::thread th2([=]() {
    // Worker
    Mailbox mailbox(false, node, 1);
    mailbox.Start();
    mailbox.Stop();
  });
  th1.join();
  th2.join();
}

}  // namespace
}  // namespace xyz
