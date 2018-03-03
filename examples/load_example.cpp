#include "core/program_context.hpp"
#include "core/plan/mapjoin.hpp"
#include "core/engine.hpp"

#include "gflags/gflags.h"
#include "glog/logging.h"

DEFINE_int32(num_worker, -1, "The number of workers");
DEFINE_string(scheduler, "proj10", "The host of scheduler");
DEFINE_int32(scheduler_port, 9000, "The port of scheduler");
DEFINE_string(url, "", "The url for hdfs file");
DEFINE_string(hdfs_namenode, "proj10", "The namenode of hdfs");
DEFINE_int32(hdfs_port, 9000, "The port of hdfs");

namespace xyz {

struct ObjT {
  using KeyT = std::string;
  using ValT = int;
  ObjT() = default;
  ObjT(KeyT key) : a(key), b(0) {}
  KeyT Key() const { return a; }
  KeyT a;
  int b;
  friend SArrayBinStream& operator<<(xyz::SArrayBinStream& stream, const ObjT& obj) {
    stream << obj.a << obj.b;
    return stream;
  }
  friend SArrayBinStream& operator>>(xyz::SArrayBinStream& stream, ObjT& obj) {
    stream >> obj.a >> obj.b;
    return stream;
  }
};

void Run() {
  // 1. construct the plan
  int plan_id = 0;
  Collection<std::string, SeqPartition<std::string>> c1{1};
  c1.Load(FLAGS_url, [](std::string& s) { return s; });

  Collection<ObjT> c2{2};
  auto plan = GetMapJoin<int>(plan_id, c1, c2);
  plan.map = [](const std::string& a) {
    return std::pair<std::string, int>(a, 1);
  };
  plan.join = [](ObjT* obj, int m) {
    obj->b += m;
  };
  ProgramContext program;
  // program.plans.push_back(plan.GetPlanSpec());
  program.collections.push_back(c1.GetSpec());
  program.collections.push_back(c2.GetSpec());

  // 2. create engine and register the plan
  Engine::Config config;
  config.num_workers = FLAGS_num_worker;
  config.scheduler = FLAGS_scheduler;
  config.scheduler_port = FLAGS_scheduler_port;
  config.num_threads = 1;
  config.namenode = FLAGS_hdfs_namenode;
  config.port = FLAGS_hdfs_port;

  Engine engine;
  // initialize the components and actors,
  // especially the function_store, to be registed by the plan
  engine.Init(config);
  // register program containing plan and collection info
  engine.RegisterProgram(program);
  // add related functions
  engine.AddFunc(plan);
  engine.AddFunc(c1);
  engine.AddFunc(c2);

  // start the mailbox and start to receive messages
  engine.Start();
  // stop the mailbox and actors
  engine.Stop();
}

}  // namespace xyz

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  CHECK_NE(FLAGS_num_worker, -1);
  CHECK(!FLAGS_scheduler.empty());

  xyz::Run();
}
