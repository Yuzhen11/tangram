#include "gtest/gtest.h"
#include "glog/logging.h"

#include "scheduler.hpp"
#include "comm/simple_sender.hpp"

namespace xyz {
namespace {

class TestScheduler : public testing::Test {};

std::vector<Node> GetNodes() {
  Node n1, n2;
  n1.id = 1;
  n2.id = 2;
  std::vector<Node> nodes{n1, n2};
  return nodes;
}
TEST_F(TestScheduler, Create) {
  const int qid = 0;
  auto nodes = GetNodes();
  auto sender = std::make_shared<SimpleSender>();
  Scheduler scheduler(qid, sender);
}

TEST_F(TestScheduler, RegisterProgram) {
  const int qid = 0;
  auto nodes = GetNodes();
  auto sender = std::make_shared<SimpleSender>();
  Scheduler scheduler(qid, sender);
  scheduler.Ready(nodes);
  auto* q = scheduler.GetWorkQueue();

  // program
  ProgramContext program;
  const int pid = 0;
  const int mid = 1;
  const int jid = 2;
  const int wid = -1;
  const int num_parts = 10;
  PlanSpec plan{pid, mid, jid, wid};
  CollectionView c1{mid, num_parts};
  CollectionView c2{jid, num_parts};
  program.plans.push_back(plan);
  program.collections.push_back(c1);
  program.collections.push_back(c2);

  SArrayBinStream ctrl_bin, bin;
  ctrl_bin << ScheduleFlag::kRegisterProgram;
  bin << program;
  Message msg;
  msg.meta.sender = 10;
  msg.meta.recver = 0;
  msg.meta.flag = Flag::kOthers;
  msg.AddData(ctrl_bin.ToSArray());
  msg.AddData(bin.ToSArray());
  q->Push(msg);

  q->Push(msg);  // register another time

  {
    auto msg = sender->Get();
    EXPECT_EQ(msg.meta.recver, 10);
    ASSERT_EQ(msg.data.size(), 2);
    SArrayBinStream ctrl_bin;
    ctrl_bin.FromSArray(msg.data[0]);
    ScheduleFlag flag;
    ctrl_bin >> flag;
    EXPECT_EQ(flag, ScheduleFlag::kInitWorkers);
  }
  {
    auto msg = sender->Get();
    EXPECT_EQ(msg.meta.recver, 20);
    ASSERT_EQ(msg.data.size(), 2);
    SArrayBinStream ctrl_bin;
    ctrl_bin.FromSArray(msg.data[0]);
    ScheduleFlag flag;
    ctrl_bin >> flag;
    EXPECT_EQ(flag, ScheduleFlag::kInitWorkers);
  }
  ASSERT_EQ(sender->msgs.Size(), 0);
}

}  // namespace
}  // namespace xyz

