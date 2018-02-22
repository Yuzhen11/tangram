#include "gflags/gflags.h"
#include "glog/logging.h"

//#include "base/node_util.hpp"
#include "comm/mailbox.hpp"

DEFINE_int32(my_id, -1, "The process id of this program");
DEFINE_string(config_file, "", "The config file path");
DEFINE_int32(num_worker, -1, "The number of workers");
DEFINE_string(scheduler, "", "The host of scheduler");
DEFINE_string(scheduler_port, "", "The port of scheduler");

namespace xyz {

void Run() {
    /* 0. Basic checks */
    CHECK_NE(FLAGS_my_id, -1);
    CHECK(!FLAGS_config_file.empty());
    CHECK_NE(FLAGS_num_worker, -1);
    CHECK(!FLAGS_scheduler.empty());
    CHECK(!FLAGS_scheduler_port.empty());
    VLOG(1) << FLAGS_my_id << " " << FLAGS_config_file;

    /* 1. Parse config_file */
    //Node scheduler_node = ParseFile(FLAGS_config_file); // Scheduler's id is 0
    Node scheduler_node{0, FLAGS_scheduler, std::stoi(FLAGS_scheduler_port), false};
    //CHECK(CheckValidNodeId(scheduler_node));
    //CHECK(CheckUniquePort(FLAGS_scheduler_port));
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
