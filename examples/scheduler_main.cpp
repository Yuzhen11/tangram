#include "gflags/gflags.h"
#include "glog/logging.h"

#include "comm/scheduler_mailbox.hpp"
#include "comm/sender.hpp"
#include "core/scheduler/scheduler.hpp"
#include "io/assigner.hpp"
#include "io/hdfs_browser.hpp"

DEFINE_int32(num_worker, -1, "The number of workers");
DEFINE_string(scheduler, "proj10", "The host of scheduler");
DEFINE_int32(scheduler_port, -1, "The port of scheduler");
DEFINE_string(hdfs_namenode, "proj10", "The namenode of hdfs");
DEFINE_int32(hdfs_port, 9000, "The port of hdfs");

DEFINE_string(dag_runner_type, "sequential", "");

namespace xyz {

void RunScheduler() {
  Node scheduler_node{0, FLAGS_scheduler, FLAGS_scheduler_port, false};

  // create mailbox and sender
  auto scheduler_mailbox =
      std::make_shared<SchedulerMailbox>(scheduler_node, FLAGS_num_worker);
  auto sender = std::make_shared<Sender>(-1, scheduler_mailbox.get());

  // create scheduler and register queue
  const int id = 0;
  const std::string namenode = FLAGS_hdfs_namenode;
  const int port = FLAGS_hdfs_port;
  auto assigner_builder = [sender, namenode, port]() {
    auto browser = std::make_shared<HDFSBrowser>(namenode, port);
    auto assigner = std::make_shared<Assigner>(sender, browser);
    return assigner;
  };
  Scheduler scheduler(id, sender, assigner_builder, FLAGS_dag_runner_type);
  scheduler_mailbox->RegisterQueue(id, scheduler.GetWorkQueue());

  // start mailbox
  scheduler_mailbox->Start();

  // make scheduler ready
  auto nodes = scheduler_mailbox->GetNodes();
  CHECK_GT(nodes.size(), 0);
  scheduler.Ready(nodes);

  scheduler.Wait();
  scheduler_mailbox->Stop();
}

} // namespace xyz

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  CHECK_NE(FLAGS_num_worker, -1);
  CHECK(!FLAGS_scheduler.empty());

  xyz::RunScheduler();
}
