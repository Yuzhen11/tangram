#pragma once

#include "core/plan/collection.hpp"
#include "core/plan/plan_base.hpp"

#include "core/plan/mapjoin.hpp"
#include "core/plan/distribute.hpp"
#include "core/plan/load.hpp"

namespace xyz {

template <typename T>
struct return_type : return_type<decltype(&T::operator())>
{};
// For generic types, directly use the result of the signature of its 'operator()'

template <typename ClassType, typename ReturnType, typename... Args>
struct return_type<ReturnType(ClassType::*)(Args...) const>
{
    using type = ReturnType;
};

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

  template<typename D>
  static auto* distribute(std::vector<D>&& data, int num_parts = 1) {
    auto* c = collections_.make<Collection<D, SeqPartition<D>>>(num_parts);
    auto* p = plans_.make<Distribute<D>>(c->Id(), num_parts);
    p->data = std::move(data);

    return c;
  }

  template<typename Parse>
  static auto* load(std::string url, Parse parse) {
    using D = decltype(parse(*(std::string*)nullptr));
    auto* c = collections_.make<Collection<D, SeqPartition<D>>>();
    auto* p = plans_.make<Load<D>>(c->Id(), url, parse);
    return c;
  }

  template<typename D>
  static auto* placeholder(int num_parts = 1) {
    auto* c = collections_.make<Collection<D>>(num_parts);
    c->SetMapper(std::make_shared<HashKeyToPartMapper<typename D::KeyT>>(num_parts));
    auto* p = plans_.make<Distribute<D, IndexedSeqPartition<D>>>(c->Id(), num_parts);
    return c;
  }

  template<typename C1, typename C2, typename M, typename J>
  static auto* mapjoin(C1* c1, C2* c2, M m, J j, int num_iter = 1, 
    typename std::enable_if_t<std::is_object<typename return_type<M>::type::second_type>::value>* = 0) {
    using MsgT = typename decltype(m(*(typename C1::ObjT*)nullptr))::second_type;
    auto *p = plans_.make<MapJoin<C1, C2, typename C1::ObjT, typename C2::ObjT, MsgT>>(c1, c2);
    p->map = m;
    p->join = j;
    p->num_iter = num_iter;
    return p;
  }

  template<typename C1, typename C2, typename M, typename J>
  static auto* mapjoin(C1* c1, C2* c2, M m, J j, int num_iter = 1, 
    typename std::enable_if_t<std::is_object<typename return_type<M>::type::value_type>::value >* = 0) {
    using MsgT = typename decltype(m(*(typename C1::ObjT*)nullptr))::value_type::second_type;
    auto *p = plans_.make<MapJoin<C1, C2, typename C1::ObjT, typename C2::ObjT, MsgT>>(c1, c2);
    p->map_vec = m;
    p->join = j;
    p->num_iter = num_iter;
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

