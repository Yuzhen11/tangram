#include "core/plan/runner.hpp"

#include "base/color.hpp"

DEFINE_string(scheduler, "", "The host of scheduler");
DEFINE_int32(scheduler_port, -1, "The port of scheduler");
DEFINE_string(hdfs_namenode, "proj10", "The namenode of hdfs");
DEFINE_int32(hdfs_port, 9000, "The port of hdfs");
DEFINE_int32(num_local_threads, 1, "# local_threads");

using namespace xyz;

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

int main(int argc, char** argv) {
  Runner::Init(argc, argv);
  std::vector<std::string> seed;
  const int num_map_part = 5;
  const int num_join_part = num_map_part;
  for (int i = 0; i < num_map_part; ++ i) {
    seed.push_back(std::to_string(i));
  }

  auto c1 = Context::distribute(seed, num_map_part);

  auto c2 = Context::placeholder<ObjT>(num_join_part);

  // mapjoin
  Context::mapjoin(c1, c2, 
    [](std::string word) {
      int id = std::stoi(word);
      LOG(INFO) << GREEN("id: "+word+", sleep for: " +std::to_string(500*id) + " ms");
      std::this_thread::sleep_for(std::chrono::milliseconds(500*id));
      return std::pair<std::string, int>(word, 1);
    },
    [](ObjT* obj, int m) {
      obj->b += m;
      LOG(INFO) << "join result: " << obj->a << " " << obj->b;
    });

  Runner::Run();
  // Runner::PrintDag();
}
