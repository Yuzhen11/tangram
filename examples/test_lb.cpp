#include "core/plan/runner.hpp"

#include "base/color.hpp"

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

void mr() {
  std::vector<int> seed;
  const int num_map_part = 5;
  const int num_update_part = num_map_part;
  for (int i = 0; i < num_map_part; ++i) {
    seed.push_back(i);
  }
  auto c1 = Context::distribute(seed, num_map_part);
  auto c2 = Context::placeholder<ObjT>(num_update_part);

  // mapupdate
  Context::mapupdate(
      c1, c2,
      [](int id, Output<int, int> *o) {
        LOG(INFO) << GREEN("id: " + std::to_string(id) + ", sleep for: " +
                           std::to_string(500 * id) + " ms");
        std::this_thread::sleep_for(std::chrono::milliseconds(500 * id));
        o->Add(id, 1);
      },
      [](ObjT *obj, int m) {
        obj->b += m;
        LOG(INFO) << "update result: " << obj->a << " " << obj->b;
      });
}

void pr() {
  std::vector<ObjT> seed;
  const int num_map_part = 5;
  const int num_update_part = num_map_part;
  for (int i = 0; i < num_map_part; ++i) {
    seed.push_back(ObjT(i));
  }
  auto c1 = Context::distribute_by_key(seed, num_map_part);

  // mapupdate
  Context::mapupdate(c1, c1,
                   [](ObjT obj, Output<int, int> *o) {
                     int id = obj.a;
                     // LOG(INFO) << GREEN("id: "+word+", sleep for: "
                     // +std::to_string(500*id) + " ms");
                     std::this_thread::sleep_for(
                         std::chrono::milliseconds(1000 * id));
                     for (int i = 0; i < num_map_part; ++i) {
                       o->Add(i, obj.a);
                     }
                   },
                   [](ObjT *obj, int m) {
                     LOG(INFO) << "part " << obj->a << " received part " << m;
                   });
}

int main(int argc, char **argv) {
  Runner::Init(argc, argv);

  // mr();
  pr();

  Runner::Run();
  // Runner::PrintDag();
}
