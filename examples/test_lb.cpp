#include "core/plan/runner.hpp"

#include "base/color.hpp"

DEFINE_string(scheduler, "", "The host of scheduler");
DEFINE_int32(scheduler_port, -1, "The port of scheduler");
DEFINE_string(hdfs_namenode, "proj10", "The namenode of hdfs");
DEFINE_int32(hdfs_port, 9000, "The port of hdfs");
DEFINE_int32(num_local_threads, 1, "# local_threads");

using namespace xyz;

struct ObjT {
  using KeyT = int;
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


void mr() {
  std::vector<int> seed;
  const int num_map_part = 5;
  const int num_join_part = num_map_part;
  for (int i = 0; i < num_map_part; ++ i) {
    seed.push_back(i);
  }
  auto c1 = Context::distribute(seed, num_map_part);
  auto c2 = Context::placeholder<ObjT>(num_join_part);

  // mapjoin
  Context::mapjoin(c1, c2, 
    [](int id) {
      LOG(INFO) << GREEN("id: "+std::to_string(id)+", sleep for: " +std::to_string(500*id) + " ms");
      std::this_thread::sleep_for(std::chrono::milliseconds(500*id));
      return std::pair<int, int>(id, 1);
    },
    [](ObjT* obj, int m) {
      obj->b += m;
      LOG(INFO) << "join result: " << obj->a << " " << obj->b;
    });
}

void pr() {
  std::vector<ObjT> seed;
  const int num_map_part = 5;
  const int num_join_part = num_map_part;
  for (int i = 0; i < num_map_part; ++ i) {
    seed.push_back(ObjT(i));
  }
  auto c1 = Context::distribute_by_key(seed, num_map_part);

  // mapjoin
  Context::mapjoin(c1, c1, 
    [](ObjT obj) {
      int id = obj.a;
      // LOG(INFO) << GREEN("id: "+word+", sleep for: " +std::to_string(500*id) + " ms");
      std::this_thread::sleep_for(std::chrono::milliseconds(1000*id));
      // return std::pair<int, int>(word, 1);
      std::vector<std::pair<int, int>> ret;
      for (int i = 0; i < num_map_part; ++ i) {
        ret.push_back({i, obj.a});
      }
      return ret;
    },
    [](ObjT* obj, int m) {
      LOG(INFO) << "part " << obj->a << " received part " << m;
    });
}

int main(int argc, char** argv) {
  Runner::Init(argc, argv);

  //mr();
  pr();

  Runner::Run();
  // Runner::PrintDag();
}
