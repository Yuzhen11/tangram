#include "gflags/gflags.h"
#include "glog/logging.h"

//#include "base/node_util.hpp"
#include "comm/mailbox.hpp"

//DEFINE_int32(my_id, -1, "The process id of this program");
//DEFINE_string(config_file, "", "The config file path");
DEFINE_int32(num_workers, 1, "The number of workers");

namespace xyz {

void Run() {
  /* 0. Basic checks */
  //CHECK_NE(FLAGS_my_id, -1);
  //CHECK(!FLAGS_config_file.empty());
  //VLOG(1) << FLAGS_my_id << " " << FLAGS_config_file;

  /* 1. Parse config_file */
  //Node scheduler_node = ParseFile(FLAGS_config_file); // Scheduler's id is 0
  Node scheduler_node{0, "localhost", 32145, false};
  //CHECK(CheckValidNodeId(scheduler_node));
  //CHECK(CheckUniquePort(scheduler_node));
  //CHECK(CheckConsecutiveIds(nodes));
  LOG(INFO) << "scheduler_node: " << scheduler_node.DebugString();

  // 1. Scheduler
  std::thread th1([=]() {
    Mailbox scheduler_mailbox(true, scheduler_node, FLAGS_num_workers);
    scheduler_mailbox.Start();
    scheduler_mailbox.Stop();
    sleep(3);
  });

  // 2. Workers
  std::thread th2([=]() {
    std::vector<Mailbox*> worker_mailboxs; 
    int num_workers = FLAGS_num_workers;
    LOG(INFO) << "num_workers: " << std::to_string(num_workers);

    for (int i = 0; i < FLAGS_num_workers; ++i) {
	    worker_mailboxs.push_back(new Mailbox(false, scheduler_node, FLAGS_num_workers));
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
  });

  th1.join();
  th2.join();
}

}  // namespace xyz

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  xyz::Run();
}
