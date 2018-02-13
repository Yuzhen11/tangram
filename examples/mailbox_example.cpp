#include "gflags/gflags.h"
#include "glog/logging.h"

//#include "base/node_util.hpp"
#include "comm/mailbox.hpp"

DEFINE_int32(my_id, -1, "The process id of this program");
DEFINE_string(config_file, "", "The config file path");
DEFINE_int32(num_worker, 1, "The number of workers");
DEFINE_int32(num_worker_node, 1, "The number of workers");

namespace xyz {

void Run() {
  /* 0. Basic checks */
  CHECK_NE(FLAGS_my_id, -1);
  CHECK(!FLAGS_config_file.empty());
  VLOG(1) << FLAGS_my_id << " " << FLAGS_config_file;

  /* 1. Parse config_file */
  //Node scheduler_node = ParseFile(FLAGS_config_file); // Scheduler's id is 0
  Node scheduler_node{0, "localhost", 32145, false};
  //CHECK(CheckValidNodeId(scheduler_node));
  //CHECK(CheckUniquePort(scheduler_node));
  //CHECK(CheckConsecutiveIds(nodes));
  LOG(INFO) << "scheduler_node: " << scheduler_node.DebugString();

  // 1. Scheduler
  if (FLAGS_my_id == 0) {
    Mailbox scheduler_mailbox(true, scheduler_node, FLAGS_num_worker);
    scheduler_mailbox.Start();
    scheduler_mailbox.Stop();
    sleep(3);
  }

  // 2. Workers
  else {
    std::vector<Mailbox*> worker_mailboxs; 
    int num_worker_node = FLAGS_num_worker_node;
    LOG(INFO) << "num_worker_node: " << std::to_string(num_worker_node);

    int num_worker_in_this_node = (FLAGS_my_id <= FLAGS_num_worker % FLAGS_num_worker_node) ? (FLAGS_num_worker / FLAGS_num_worker_node + 1) : (FLAGS_num_worker / FLAGS_num_worker_node);

    for (int i = 0; i < num_worker_in_this_node; ++i) {
      worker_mailboxs.push_back(new Mailbox(false, scheduler_node, FLAGS_num_worker));
    }

    for (auto mailbox : worker_mailboxs) {
        mailbox->Start();
    }

    // 3. Stop
    for (auto mailbox : worker_mailboxs) {
        mailbox->Stop();
    }

    // clean the pointers
    for (auto it = worker_mailboxs.begin() ; it != worker_mailboxs.end(); ++it)
        delete *it;
    worker_mailboxs.clear();
  }
}

}  // namespace xyz

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  xyz::Run();
}
