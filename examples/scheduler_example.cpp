#include "gflags/gflags.h"
#include "glog/logging.h"

//#include "base/node_util.hpp"
#include "comm/mailbox.hpp"

DEFINE_int32(num_worker, -1, "The number of workers");
DEFINE_string(scheduler, "", "The host of scheduler");
DEFINE_string(scheduler_port, "", "The port of scheduler");

namespace xyz {

void Run() {
    /* 0. Basic checks */
    CHECK_NE(FLAGS_num_worker, -1);
    CHECK(!FLAGS_scheduler.empty());
    CHECK(!FLAGS_scheduler_port.empty());

    /* 1. Parse config_file */
    Node scheduler_node{0, FLAGS_scheduler, std::stoi(FLAGS_scheduler_port), false};
    LOG(INFO) << "scheduler_node: " << scheduler_node.DebugString();

    /* 1. Scheduler program */
    Mailbox scheduler_mailbox(true, scheduler_node, FLAGS_num_worker);
    scheduler_mailbox.Start();
    // scheduler_mailbox.Stop();
    std::this_thread::sleep_for(std::chrono::seconds(10));
    scheduler_mailbox.Stop();
}

}  // namespace xyz

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  xyz::Run();
}
