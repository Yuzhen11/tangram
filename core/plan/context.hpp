#pragma once
#include "base/third_party/range.h"

#include "core/plan/collection.hpp"
#include "core/plan/plan_base.hpp"

#include "core/plan/mapjoin.hpp"
#include "core/plan/mapwithjoin.hpp"
#include "core/plan/distribute.hpp"
#include "core/plan/load.hpp"
#include "core/plan/write.hpp"
#include "core/plan/checkpoint.hpp"
#include "core/plan/load_checkpoint.hpp"

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

struct CountObjT {
  using KeyT = int;
  using ValT = int;
  CountObjT() = default;
  CountObjT(KeyT key) : a(key), b(0) {}
  KeyT Key() const { return a; }
  KeyT a;
  int b;
  friend SArrayBinStream& operator<<(xyz::SArrayBinStream& stream, const CountObjT& obj) {
    stream << obj.a << obj.b;
    return stream;
  }
  friend SArrayBinStream& operator>>(xyz::SArrayBinStream& stream, CountObjT& obj) {
    stream >> obj.a >> obj.b;
    return stream;
  }
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

  template<typename C, typename F>
  static void write(C* c, std::string url, F write) {
    plans_.make<Write<typename C::ObjT>>(c->Id(), url, write);
  }

  template<typename C>
  static void checkpoint(C* c, std::string url) {
    plans_.make<Checkpoint>(c->Id(), url);
  }

  template<typename C>
  static void loadcheckpoint(C* c, std::string url) {
    plans_.make<LoadCheckpoint>(c->Id(), url);
  }

  template<typename D>
  static auto* placeholder(int num_parts = 1) {
    auto* c = collections_.make<Collection<D>>(num_parts);
    c->SetMapper(std::make_shared<HashKeyToPartMapper<typename D::KeyT>>(num_parts));
    auto* p = plans_.make<Distribute<D, IndexedSeqPartition<D>>>(c->Id(), num_parts);
    return c;
  }

  template<typename D>
  static auto* placeholder(std::vector<third_party::Range> ranges) {
    auto* c = collections_.make<Collection<D>>(ranges.size());
    c->SetMapper(std::make_shared<RangeKeyToPartMapper<typename D::KeyT>>(ranges));
    auto* p = plans_.make<Distribute<D, IndexedSeqPartition<D>>>(c->Id(), ranges.size());
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
  
  template<typename C1, typename C2, typename M, typename J>
  static auto* mappartjoin(C1* c1, C2* c2, M m, J j, int num_iter = 1) {
    using MsgT = typename decltype(m((TypedPartition<typename C1::ObjT>*)nullptr, (AbstractMapProgressTracker*)nullptr))::value_type::second_type;
    auto *p = plans_.make<MapPartJoin<C1, C2, typename C1::ObjT, typename C2::ObjT, MsgT>>(c1, c2);
    p->mappart = m;
    p->join = j;
    p->num_iter = num_iter;
    return p;
  }

  template<typename C1>
  static void count(C1* c1) {
    auto *count_collection = placeholder<CountObjT>(1);
    auto *p = plans_.make<MapJoin<C1, Collection<CountObjT>, typename C1::ObjT, CountObjT, int>>(c1, count_collection);
    p->map = [](const typename C1::ObjT& obj) {
      return std::make_pair(0, 1);
    };
    p->join = [](CountObjT* a, int b) {
      a->b += b;
    };
    p->num_iter = 1;
    auto *p2 = plans_.make<MapJoin<Collection<CountObjT>, Collection<CountObjT>, 
         CountObjT, CountObjT, int>>(count_collection, count_collection);
    p2->map = [](const CountObjT& obj) {
      LOG(INFO) << "**************count: " << obj.b;
      return std::make_pair(0, 0);
    };
    p2->join = [](CountObjT* a, int b) {
    };
    p2->num_iter = 1;
  }

  template<typename C1, typename C2, typename C3, typename M, typename J>
  static auto* mappartwithjoin(C1* c1, C2* c2, C3* c3, M m, J j, int num_iter = 1) {
    using MsgT = typename decltype(
            m((TypedPartition<typename C1::ObjT>*)nullptr, 
              (TypedCache<typename C2::ObjT>*)nullptr, 
              (AbstractMapProgressTracker*)nullptr)
            )::value_type::second_type;
    auto *p = plans_.make<MapPartWithJoin<C1, C2, C3, typename C1::ObjT, typename C2::ObjT, typename C3::ObjT, MsgT>>(c1, c2, c3);
    p->mappartwith = m;
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

