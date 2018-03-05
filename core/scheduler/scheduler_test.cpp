#include "glog/logging.h"
#include "gtest/gtest.h"

#include "comm/simple_sender.hpp"
#include "core/queue_node_map.hpp"
#include "scheduler.hpp"

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

TEST_F(TestScheduler, RegisterProgramAndInitWorker) {
  const int qid = 0;
  auto nodes = GetNodes();
  auto sender = std::make_shared<SimpleSender>();
  Scheduler scheduler(qid, sender);
  scheduler.Ready(nodes);
  auto *q = scheduler.GetWorkQueue();

  // program
  ProgramContext program;
  // const int pid = 0;
  // const int mid = 1;
  // const int jid = 2;
  const int num_parts = 1;
  // PlanSpec plan{pid, mid, jid};
  CollectionSpec c1{0, num_parts};
  // CollectionSpec c2{jid, num_parts};
  // program.plans.push_back(plan);
  program.collections.push_back(c1);
  // program.collections.push_back(c2);

  WorkerInfo info;
  info.num_local_threads = 1;
  SArrayBinStream ctrl_bin, bin;
  ctrl_bin << ScheduleFlag::kRegisterProgram;
  bin << info << program;
  Message msg;
  msg.meta.sender = GetWorkerQid(1);
  msg.meta.recver = 0;
  msg.meta.flag = Flag::kOthers;
  msg.AddData(ctrl_bin.ToSArray());
  msg.AddData(bin.ToSArray());
  q->Push(msg);

  q->Push(msg); // register another time

  {
    // recv kDistribute
    auto msg = sender->Get();
    ASSERT_EQ(msg.data.size(), 2);
    SArrayBinStream ctrl_bin;
    ctrl_bin.FromSArray(msg.data[0]);
    ScheduleFlag flag;
    ctrl_bin >> flag;
    EXPECT_EQ(flag, ScheduleFlag::kDistribute);
  }
  {
    // send
    Message msg;
    msg.meta.sender = GetWorkerQid(1);
    msg.meta.recver = 0;
    msg.meta.flag = Flag::kOthers;
    SArrayBinStream ctrl_bin, bin;
    ctrl_bin << ScheduleFlag::kFinishDistribute;
    bin << int(0) << int(0) << int(1);
    msg.AddData(ctrl_bin.ToSArray());
    msg.AddData(bin.ToSArray());
    q->Push(msg);
  }
  {
    auto msg = sender->Get();
    EXPECT_EQ(msg.meta.recver, GetWorkerQid(1));
    ASSERT_EQ(msg.data.size(), 2);
    SArrayBinStream ctrl_bin;
    ctrl_bin.FromSArray(msg.data[0]);
    ScheduleFlag flag;
    ctrl_bin >> flag;
    EXPECT_EQ(flag, ScheduleFlag::kInitWorkers);
  }
  {
    auto msg = sender->Get();
    EXPECT_EQ(msg.meta.recver, GetWorkerQid(2));
    ASSERT_EQ(msg.data.size(), 2);
    SArrayBinStream ctrl_bin;
    ctrl_bin.FromSArray(msg.data[0]);
    ScheduleFlag flag;
    ctrl_bin >> flag;
    EXPECT_EQ(flag, ScheduleFlag::kInitWorkers);
  }
  // send InitWorkersReply
  {
    SArrayBinStream ctrl_bin, bin;
    ctrl_bin << ScheduleFlag::kInitWorkersReply;
    Message msg;
    msg.meta.sender = GetWorkerQid(1);
    msg.meta.recver = 0;
    msg.meta.flag = Flag::kOthers;
    msg.AddData(ctrl_bin.ToSArray());
    msg.AddData(bin.ToSArray());
    q->Push(msg);

    msg.meta.sender = GetWorkerQid(2);
    q->Push(msg); // register another time
  }
}

/*
TEST_F(TestScheduler, CheckPoint) {
  const int qid = 0;
  auto nodes = GetNodes();
  auto sender = std::make_shared<SimpleSender>();
  Scheduler scheduler(qid, sender);
  scheduler.Ready(nodes);
  scheduler.CheckPoint();

  auto msg = sender->Get();
  ASSERT_EQ(msg.data.size(), 2);
  SArrayBinStream ctrl_bin;
  ctrl_bin.FromSArray(msg.data[0]);
  ScheduleFlag flag;
  ctrl_bin >> flag;
  EXPECT_EQ(flag, ScheduleFlag::kCheckPoint);
}
*/

} // namespace
} // namespace xyz
