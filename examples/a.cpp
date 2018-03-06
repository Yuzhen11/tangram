#include "core/plan/runner.hpp"

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
  auto c1 = Context::collection<Collection<std::string, SeqPartition<std::string>>>();
  c1->Distribute(
          std::vector<std::string>{"b", "a", "n", "a", "n", "a"});

  auto c2 = Context::collection<Collection<ObjT>>(10);
  c2->SetMapper(std::make_shared<HashKeyToPartMapper<ObjT::KeyT>>(10));

  auto p = Context::mapjoin(c1, c2, 
    [](std::string word) {
      return std::pair<std::string, int>(word, 1);
    },
    [](ObjT* obj, int m) {
      obj->b += m;
      LOG(INFO) << "join result: " << obj->a << " " << obj->b;
    });

  Runner::Run(argc, argv);
}
