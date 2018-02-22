#include "core/plan/plan.hpp"
#include "core/engine.hpp"

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
  Engine engine;
  engine.Start();
  engine.Add(plan);
  engine.Run();
  engine.Stop();

}  // namespace xyz

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  xyz::Run();
}
