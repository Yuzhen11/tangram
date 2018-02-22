#include "core/plan/plan.hpp"
#include "core/engine.hpp"

#include "gflags/gflags.h"
#include "glog/logging.h"

DEFINE_int32(num_worker, -1, "The number of workers");
DEFINE_string(scheduler, "proj10", "The host of scheduler");
DEFINE_int32(scheduler_port, 9000, "The port of scheduler");

namespace xyz {

struct ObjT {
  using KeyT = int;
  using ValT = int;
  ObjT(KeyT key) : a(key), b(0) {}
  KeyT Key() const { return a; }
  int a;
  int b;
};

void Run() {
  // 1. construct the plan
  int plan_id = 0;
  Collection<ObjT> c1{1};
  Collection<ObjT> c2{2};
  Plan<ObjT, ObjT, int> plan(plan_id, c1, c2);

  plan.map = [](ObjT a) {
    return std::pair<ObjT::KeyT, int>(a.Key(), 1);
  };
  plan.join = [](ObjT* obj, int m) {
    obj->b += m;
  };

  // 2. create engine and register the plan
  Engine::Config config;
  config.num_workers = FLAGS_num_worker;
  config.scheduler = FLAGS_scheduler;
  config.scheduler_port = FLAGS_scheduler_port;
  config.num_threads = 1;
  config.namenode = "proj10";
  config.port = 9000;

  Engine engine;
  engine.Start(config);
  // engine.Add(plan);
  // engine.Run();
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