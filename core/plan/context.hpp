#pragma once

#include "core/plan/collection.hpp"
#include "core/plan/plan_base.hpp"

#include "core/plan/mapjoin.hpp"

namespace xyz {

template <typename Base> class Store {
public:
  template <typename O = Base, typename... ArgT> 
  O *make(ArgT... args) {
    auto id = instances.size();
    O *oper = new O(id, args...);
    instances.push_back(oper);
    return oper;
  }
  auto all() { return instances; }
private:
  std::vector<Base *> instances;
};

class Context {
 public:
  template<typename C, typename... ArgT>
  static C* collection(ArgT... args) {
    return collections_.make<C>(args...);
  }

  template<typename C1, typename C2, typename M, typename J>
  static auto* mapjoin(C1* c1, C2* c2, M m, J j) {
    using MsgT = typename decltype(m(*(typename C1::ObjT*)nullptr))::second_type;
    auto *p = plans_.make<MapJoin<C1, C2, typename C1::ObjT, typename C2::ObjT, MsgT>>(c1, c2);
    p->map = m;
    p->join = j;
    return p;
  }
  
  static auto get_allplans() {
    return plans_.all();
  }
  static auto get_allcollections() {
    return collections_.all();
  }
 private:
  static Store<CollectionBase> collections_;
  static Store<PlanBase> plans_;
};

} // namespace xyz

