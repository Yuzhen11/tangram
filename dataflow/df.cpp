#include <functional>
#include <iostream>
#include <typeinfo>
#include <vector>

#include "df.hpp"

using namespace nova;

// define
Store<Dataflow> Dataflow::dataflows;

struct ObjT {
  using KeyT = int;
};

struct MsgT {};

int main() {
  {
    auto a = distribute<int>();
    auto b = a.map([](int) { return std::pair<int, MsgT>(1, MsgT()); });
    auto c = distribute<ObjT>();
    c.apply(b, [](ObjT *, MsgT) {});

    auto *p = Dataflow::dataflows.get(0);
    p->visit();
  }

  /*
  {
  auto c = distribute<int>();
  auto b = c.map([](int) { return std::pair<int, MsgT>(1,MsgT()); });

  auto a = distribute<ObjT>();
  auto d = a.apply(b, [](ObjT*, MsgT) {})
   .map([](ObjT) { return 1; });
  }
  */
}
