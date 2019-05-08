#include "core/plan/runner.hpp"

using namespace xyz;

struct ObjT {
  using KeyT = int;
  using ValT = int;
  ObjT() = default;
  ObjT(KeyT key) : a(key), b(0) {}
  KeyT Key() const { return a; }
  KeyT a;
  int b;
  friend SArrayBinStream &operator<<(xyz::SArrayBinStream &stream,
                                     const ObjT &obj) {
    stream << obj.a << obj.b;
    return stream;
  }
  friend SArrayBinStream &operator>>(xyz::SArrayBinStream &stream, ObjT &obj) {
    stream >> obj.a >> obj.b;
    return stream;
  }
};

void mj() {
  std::vector<int> seed;
  // 2 map parts and 1 update part
  //
  // 1. kill the first machine to simulate losing update partitions
  // 2. kill the second machine to simulate not losing update partitions
  const int num_map_part = 2;
  const int num_update_part = 1;
  for (int i = 0; i < num_map_part; ++i) {
    seed.push_back(i);
  }
  auto c1 = Context::distribute(seed, num_map_part);
  auto c2 = Context::placeholder<ObjT>(num_update_part);

  Context::checkpoint(c1, "/tmp/tmp/yuzhen/c0");
  Context::checkpoint(c2, "/tmp/tmp/yuzhen/c1");

  // mapupdate
  Context::mapupdate(
      c1, c2,
      [](int id, Output<int, int> *o) {
        LOG(INFO) << GREEN("id: " + std::to_string(id) + ", sleep for: " +
                           std::to_string(2000) + " ms");
        std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        o->Add(id, 1);
      },
      [](ObjT *obj, int m) {
        obj->b += m;
        LOG(INFO) << "update result: " << obj->a << " " << obj->b;
      })
      ->SetIter(10)
      ->SetStaleness(0)
      ->SetCheckpointInterval(2, "/tmp/yuzhen/tmp/");
}

void mpj() {
  std::vector<int> seed;
  const int num_map_part = 2;
  const int num_update_part = num_map_part;
  for (int i = 0; i < num_map_part; ++i) {
    seed.push_back(i);
  }
  auto c0 = Context::distribute(seed, num_map_part);
  auto c1 = Context::placeholder<ObjT>(num_update_part);

  Context::checkpoint(c0, "/tmp/tmp/jasper/c0");
  Context::checkpoint(c1, "/tmp/tmp/jasper/c1");

  // mappartupdate
  Context::mappartupdate(
      c0, c1,
      [](TypedPartition<int> *p, Output<int, int> *o) {
        LOG(INFO) << GREEN("Sleep for: " + std::to_string(2000) + " ms");
        std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        LOG(INFO) << GREEN("Sleep done");
        for (auto &elem : *p) {
          o->Add(elem, 1);
        }
      },
      [](ObjT *obj, int m) {
        obj->b += m;
        LOG(INFO) << "update result: " << obj->a << " " << obj->b;
      })
      ->SetIter(10)
      ->SetStaleness(0)
      ->SetCheckpointInterval(5);
}

int main(int argc, char **argv) {
  Runner::Init(argc, argv);

  mj();
  // mpj();

  Runner::Run();
  // Runner::PrintDag();
}
