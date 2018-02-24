#include "gflags/gflags.h"
#include "glog/logging.h"

#include "core/scheduler/scheduler.hpp"
#include "comm/mailbox.hpp"
#include "comm/sender.hpp"

DEFINE_int32(num_worker, -1, "The number of workers");
DEFINE_string(scheduler, "proj10", "The host of scheduler");
DEFINE_int32(scheduler_port, 9000, "The port of scheduler");

namespace xyz {

void RunScheduler() {
  Node scheduler_node{0, FLAGS_scheduler, FLAGS_scheduler_port, false};

  auto scheduler_mailbox = std::make_shared<Mailbox>(true, scheduler_node, FLAGS_num_worker);
  scheduler_mailbox->Start();
  auto nodes = scheduler_mailbox->GetNodes();
  CHECK_GT(nodes.size(), 0);
  auto sender = std::make_shared<Sender>(-1, scheduler_mailbox.get());
  // the scheduler is started after the mailbox
  const int id = 0;
  Scheduler scheduler(id, sender, nodes);
  scheduler_mailbox->RegisterQueue(id, scheduler.GetWorkQueue());
  scheduler.StartScheduling();

  std::this_thread::sleep_for(std::chrono::seconds(10));
  scheduler_mailbox->Stop();
}

}  // namespace xyz


int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  CHECK_NE(FLAGS_num_worker, -1);
  CHECK(!FLAGS_scheduler.empty());

  xyz::RunScheduler();
}
