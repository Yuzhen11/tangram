#include "gflags/gflags.h"
#include "glog/logging.h"

//#include "base/node_util.hpp"
#include "comm/worker_mailbox.hpp"

DEFINE_string(scheduler, "", "The host of scheduler");
DEFINE_string(scheduler_port, "", "The port of scheduler");

namespace xyz {

void Run() {
    /* 0. Basic checks */
    CHECK(!FLAGS_scheduler.empty());
    CHECK(!FLAGS_scheduler_port.empty());

    Node scheduler_node{0, FLAGS_scheduler, std::stoi(FLAGS_scheduler_port), false};
    LOG(INFO) << "scheduler_node: " << scheduler_node.DebugString();

    /* 2. The user program */
    WorkerMailbox worker_mailbox(scheduler_node);
    worker_mailbox.Start();
    worker_mailbox.Barrier();
    worker_mailbox.Stop();
}

}  // namespace xyz

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  xyz::Run();
}
