#include "core/program_context.hpp"
#include "core/plan/mapjoin.hpp"
// #include "core/plan/collection_builder.hpp"
#include "core/plan/collection.hpp"
#include "core/engine.hpp"

#include "gflags/gflags.h"
#include "glog/logging.h"

DEFINE_string(scheduler, "", "The host of scheduler");
DEFINE_int32(scheduler_port, -1, "The port of scheduler");
DEFINE_string(hdfs_namenode, "proj10", "The namenode of hdfs");
DEFINE_int32(hdfs_port, 9000, "The port of hdfs");
DEFINE_int32(num_local_threads, 1, "# local_threads");

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
  std::vector<std::string> data;
  for (int i = 0; i < 10; ++ i) {
    data.push_back("a");
    data.push_back("b");
    data.push_back("c");
    data.push_back("d");
    data.push_back("e");
  }
  Collection<std::string, SeqPartition<std::string>> c1(1, 3);
  c1.Distribute(data);
  int num_part = 10;
  Collection<ObjT> c2{2, num_part}; // id, num_part
  c2.SetMapper(std::make_shared<HashKeyToPartMapper<ObjT::KeyT>>(num_part));

  int plan_id = 0;
  // MapJoin<std::string, ObjT, int> plan(plan_id, c1, c2);
  auto plan = GetMapJoin<int>(plan_id, &c1, &c2);
  plan.map = [](std::string word) {
    return std::pair<std::string, int>(word, 1);
  };
  plan.join = [](ObjT* obj, int m) {
    obj->b += m;
    LOG(INFO) << "join result: " << obj->a << " " << obj->b;
  };
  plan.combine = [](int m, int n){
    //LOG(INFO) << "combine result: " << m + n;
    return m + n;
  };

  ProgramContext program;
  auto plan_spec = plan.GetPlanSpec();
  plan_spec.num_iter = 5;
  program.plans.push_back(plan_spec);
  program.collections.push_back(c1.GetSpec());
  program.collections.push_back(c2.GetSpec());

  // 2. create engine and register the plan
  Engine::Config config;
  config.scheduler = FLAGS_scheduler;
  config.scheduler_port = FLAGS_scheduler_port;
  config.num_threads = FLAGS_num_local_threads;
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

  CHECK(!FLAGS_scheduler.empty());

  xyz::Run();
}
