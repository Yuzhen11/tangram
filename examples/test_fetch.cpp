#include "core/plan/runner.hpp"

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
  auto c1 = Context::distribute(
      std::vector<std::string>{"b", "a", "n", "a", "n", "a"}, 1);

  auto c2 = Context::placeholder<ObjT>(10);
  auto c3 = Context::placeholder<ObjT>(10);

  // mapjoin
  Context::mapjoin(c1, c2, [](std::string word,
                              Output<std::string, int> *o) { o->Add(word, 1); },
                   [](ObjT *obj, int m) {
                     obj->b += m;
                     // LOG(INFO) << "join result: " << obj->a << " " << obj->b;
                   });

  // mappartwithjoin
  // with_collection != join_collection
  // map c1, with c2, join c3
  Context::mappartwithjoin(
      c1, c2, c3,
      [](TypedPartition<std::string> *p, TypedCache<ObjT> *typed_cache,
         Output<std::string, int> *o) {
        // GetPartition
        for (int part_id = 0; part_id < 10; ++part_id) {
          auto part = typed_cache->GetPartition(part_id);
          auto *with_p = static_cast<TypedPartition<ObjT> *>(part.get());
          LOG(INFO) << "partition: " << part_id
                    << ", size: " << with_p->GetSize();
          for (auto &elem : *with_p) {
            LOG(INFO) << "elem: " << elem.a << " " << elem.b;
          }
          typed_cache->ReleasePart(part_id);
        }

        for (auto &elem : *p) {
          o->Add(elem, 1);
        }
      },
      [](ObjT *obj, int m) {
        obj->b += m;
        LOG(INFO) << "join result: " << obj->a << " " << obj->b;
      })
      ->SetIter(10)
      ->SetStaleness(1);

  // with_collection == join_collection
  // map c1, with c2, join c2
  Context::mappartwithjoin(
      c1, c2, c2,
      [](TypedPartition<std::string> *p, TypedCache<ObjT> *typed_cache,
         Output<std::string, int> *o) {
        // GetPartition
        for (int part_id = 0; part_id < 10; ++part_id) {
          auto part = typed_cache->GetPartition(part_id);
          auto *with_p = static_cast<TypedPartition<ObjT> *>(part.get());
          LOG(INFO) << "partition: " << part_id
                    << ", size: " << with_p->GetSize();
          for (auto &elem : *with_p) {
            LOG(INFO) << "elem: " << elem.a << " " << elem.b;
          }
          typed_cache->ReleasePart(part_id);
        }

        for (auto &elem : *p) {
          o->Add(elem, 1);
        }
      },
      [](ObjT *obj, int m) {
        obj->b += m;
        LOG(INFO) << "join result: " << obj->a << " " << obj->b;
      })
      ->SetIter(10)
      ->SetStaleness(1);

  Runner::Run();
}
