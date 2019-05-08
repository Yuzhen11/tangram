#include "core/plan/runner.hpp"

DEFINE_string(url, "", "The url for hdfs file");
DEFINE_string(output_url, "", "");

using namespace xyz;

struct ObjT {
  using KeyT = std::string;
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

int main(int argc, char **argv) {
  Runner::Init(argc, argv);

  auto c1 = Context::load(FLAGS_url, [](std::string s) { return s; });
  auto c2 = Context::placeholder<ObjT>(1);

  auto p = Context::mapupdate(
      c1, c2,
      [](std::string word, Output<std::string, int> *o) { o->Add(word, 1); },
      [](ObjT *obj, int m) {
        obj->b += m;
        LOG(INFO) << "update result: " << obj->a << " " << obj->b;
      });

  Context::write(c2, FLAGS_output_url,
                 [](const ObjT &obj, std::stringstream &ss) {
                   ss << obj.a << " " << obj.b << "\n";
                 });

  Runner::Run();
}
