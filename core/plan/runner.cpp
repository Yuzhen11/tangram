#include "core/plan/runner.hpp"

DEFINE_string(scheduler, "", "The host of scheduler");
DEFINE_int32(scheduler_port, -1, "The port of scheduler");
DEFINE_string(hdfs_namenode, "", "The namenode of hdfs");
DEFINE_int32(hdfs_port, -1, "The port of hdfs");
DEFINE_int32(node_id, -1, "node id");
DEFINE_int32(num_local_threads, 20, "# local_threads");
DEFINE_int32(num_join_threads, 20, "# join_threads");
DEFINE_int32(num_combine_threads, 20, "# combine_threads");

namespace xyz {

void Runner::Init(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
}

void Runner::PrintDag() {
  LOG(INFO) << Context::get_dag().DebugString();
}

void Runner::Run() {
  CHECK(!FLAGS_scheduler.empty());

  auto plans = Context::get_allplans();
  auto collections = Context::get_allcollections();
  // TODO: replace ProgramContext with a DAG structure.
  ProgramContext program;
  // for (auto* c : collections) {
  //   program.collections.push_back(c->GetSpec());
  // }
  for (auto* p : plans) {
    program.specs.push_back(p->GetSpec());
  }
  program.dag = Context::get_dag();

  Engine::Config config;
  config.scheduler = FLAGS_scheduler;
  config.scheduler_port = FLAGS_scheduler_port;
  config.num_local_threads = FLAGS_num_local_threads;
  config.num_join_threads = FLAGS_num_join_threads;
  config.num_combine_threads = FLAGS_num_combine_threads;
  config.namenode = FLAGS_hdfs_namenode;
  config.port = FLAGS_hdfs_port;

  Engine engine;
  // initialize the components and actors,
  // especially the function_store, to be registed by the plan
  engine.Init(config);
  // register program containing plan and collection info
  engine.RegisterProgram(program);
  // add related functions
  for (auto* c : collections) {
    engine.AddFunc(c);
  }
  for (auto* p : plans) {
    engine.AddFunc(p);
  }

  // start the mailbox and start to receive messages
  engine.Start();
  // stop the mailbox and actors
  engine.Stop();
}


}  // namespace xyz

