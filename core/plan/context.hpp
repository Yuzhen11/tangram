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

#include "core/plan/dag.hpp"
#include "base/magic.hpp"

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

template <typename T>
struct function_traits
    : public function_traits<decltype(&T::operator())>
{};
// For generic types, directly use the result of the signature of its 'operator()'

template <typename ClassType, typename ReturnType, typename... Args>
struct function_traits<ReturnType(ClassType::*)(Args...) const>
// we specialize for pointers to member function
{
    enum { arity = sizeof...(Args) };
    // arity is the number of arguments.

    typedef ReturnType result_type;

    struct arg0 {
        typedef typename std::tuple_element<0, std::tuple<Args...>>::type type;
    };
    struct arg1 {
        typedef typename std::tuple_element<1, std::tuple<Args...>>::type type;
    };
    struct arg2 {
        typedef typename std::tuple_element<2, std::tuple<Args...>>::type type;
    };
    // template <size_t i>
    // struct arg
    // {
    //     typedef typename std::tuple_element<i, std::tuple<Args...>>::type type;
    //     // the i-th argument is equivalent to the i-th tuple element of a tuple
    //     // composed of those arguments.
    // };
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
  static auto* distribute(std::vector<D> data, int num_parts = 1, std::string name = "") {
    auto* c = collections_.make<Collection<D, SeqPartition<D>>>(num_parts);
    auto* p = plans_.make<Distribute<D>>(c->Id(), num_parts);
    p->data = std::move(data);
    p->name = name + "::distribute";
    dag_.AddDagNode(p->plan_id, {}, {c->Id()});
    return c;
  }

  // distribute a std::vector by key.
  // The user-defined type should have Key() function and is serializable.
  // Users should make sure there is only one key per object.
  template<typename D>
  static auto* distribute_by_key(std::vector<D> data, int num_parts = 1, std::string name = "") {
    std::string prefix = name+"::distribute_by_key";
    auto* tmp_c = distribute(data, 1, prefix);
    auto* c = placeholder<D>(num_parts, prefix)->SetName("tmp collection in distribute_by_key");
    mapjoin(tmp_c, c, 
      [](const D& d, Output<typename D::KeyT, D>* o) {
        o->Add(d.Key(), d);
      }, 
      [](D* d, D msg) {
        *d = msg;
      })->SetName(prefix+"::mapjoin");
    // TODO, remove tmp_c
    return c;
  }

  template<typename C, typename F>
  static void foreach(C* c, F f, std::string name = "") {
    std::string prefix = name + "::foreach collection " + c->Name();
    auto* tmp_c = placeholder<CountObjT>(1, prefix)->SetName("tmp collection in foreach");
    mapjoin(c, tmp_c, 
      [f](const typename C::ObjT& c, Output<int, int>* o) {
        f(c);
        o->Add(0, 0);
      },
      [](CountObjT*, int) {
        // dummy
      })->SetName(prefix+"::mapjoin");
    // TODO: remove tmp_c
  }

  template<typename Parse>
  static auto* load(std::string url, Parse parse, int max_line_per_part = -1, std::string name = "") {
    using D = decltype(parse(*(std::string*)nullptr));
    auto* c = collections_.make<Collection<D, SeqPartition<D>>>();
    auto* p = plans_.make<Load<D>>(c->Id(), url, parse, max_line_per_part, false);
    p->name = name+"::load";
    dag_.AddDagNode(p->plan_id, {}, {c->Id()});
    return c;
  }
  template<typename Parse>
  static auto* load_wholefiles(std::string url, Parse parse, int max_line_per_part = -1, std::string name = "") {
    using D = decltype(parse(*(std::string*)nullptr));
    auto* c = collections_.make<Collection<D, SeqPartition<D>>>();
    auto* p = plans_.make<Load<D>>(c->Id(), url, parse, max_line_per_part, true);
    p->name = name+"::load_wholefiles";
    dag_.AddDagNode(p->plan_id, {}, {c->Id()});
    return c;
  }

  /*
   * return a collection that need to be accessed in special way, see wordcount.cpp
   */
  static auto* load_block_meta(std::string url, std::string name = "") {
    auto* c = collections_.make<Collection<std::string, SeqPartition<std::string>>>();
    auto* p = plans_.make<Load<std::string>>(c->Id(), url, false);
    p->name = name+"::load_block_meta";
    dag_.AddDagNode(p->plan_id, {}, {c->Id()});
    return c;
  }
  static auto* load_wholefiles_meta(std::string url, std::string name = "") {
    auto* c = collections_.make<Collection<std::string, SeqPartition<std::string>>>();
    auto* p = plans_.make<Load<std::string>>(c->Id(), url, true);
    p->name = name+"::load_wholefiles_meta";
    dag_.AddDagNode(p->plan_id, {}, {c->Id()});
    return c;
  }

  template<typename C, typename F>
  static void write(C* c, std::string url, F write, std::string name = "") {
    auto* p = plans_.make<Write<typename C::ObjT>>(c->Id(), url, write);
    p->name = name+"::write";
    dag_.AddDagNode(p->plan_id, {c->Id()}, {});
  }

  template<typename C>
  static void checkpoint(C* c, std::string url, std::string name = "") {
    auto* p = plans_.make<Checkpoint>(c->Id(), url, Checkpoint::Type::checkpoint);
    p->name = name+"::checkpoint";
    dag_.AddDagNode(p->plan_id, {c->Id()}, {});
  }

  template<typename C>
  static void loadcheckpoint(C* c, std::string url, std::string name = "") {
    auto* p = plans_.make<Checkpoint>(c->Id(), url, Checkpoint::Type::loadcheckpoint);
    p->name = name+"::loadcheckpoint";
    dag_.AddDagNode(p->plan_id, {}, {c->Id()});
  }

  template<typename D>
  static auto* placeholder(int num_parts = 1, std::string name = "") {
    auto* c = collections_.make<Collection<D>>(num_parts);
    c->SetMapper(std::make_shared<HashKeyToPartMapper<typename D::KeyT>>(num_parts));
    auto* p = plans_.make<Distribute<D, IndexedSeqPartition<D>>>(c->Id(), num_parts);
    p->name = name+"::placeholder";
    dag_.AddDagNode(p->plan_id, {}, {c->Id()});
    return c;
  }

  // user can specify KeyToPartT for the placeholder
  template<typename D, typename KeyToPartT>
  static auto* placeholder(int num_parts = 1, std::string name = "") {
    auto* c = collections_.make<Collection<D>>(num_parts);
    c->SetMapper(std::make_shared<KeyToPartT>(num_parts));
    auto* p = plans_.make<Distribute<D, IndexedSeqPartition<D>>>(c->Id(), num_parts);
    p->name = name+"::placeholder";
    dag_.AddDagNode(p->plan_id, {}, {c->Id()});
    return c;
  }

  // user can specify KeyToPartT for the placeholder
  template<typename D, typename KeyToPartT>
  static auto* placeholder(int num_parts, std::shared_ptr<KeyToPartT> key_to_part_mapper, std::string name = "") {
    auto* c = collections_.make<Collection<D>>(num_parts);
    c->SetMapper(key_to_part_mapper);
    auto* p = plans_.make<Distribute<D, IndexedSeqPartition<D>>>(c->Id(), num_parts);
    p->name = name+"::placeholder";
    dag_.AddDagNode(p->plan_id, {}, {c->Id()});
    return c;
  }

  // range placeholder
  template<typename D, typename KeyToPartT>
  static auto* range_placeholder(std::shared_ptr<KeyToPartT> key_to_part_mapper, std::string name = "") {
    auto* c = collections_.make<Collection<D, RangeIndexedSeqPartition<D>>>(key_to_part_mapper->GetNumRanges());
    c->SetMapper(key_to_part_mapper);
    auto* p = plans_.make<RangeDistribute<D, KeyToPartT>>(c->Id(), key_to_part_mapper);
    p->name = name+"::range_placeholder";
    dag_.AddDagNode(p->plan_id, {}, {c->Id()});
    return c;
  }

  template<typename D>
  static auto* placeholder(std::vector<third_party::Range> ranges, std::string name = "") {
    auto* c = collections_.make<Collection<D>>(ranges.size());
    c->SetMapper(std::make_shared<RangeKeyToPartMapper<typename D::KeyT>>(ranges));
    auto* p = plans_.make<Distribute<D, IndexedSeqPartition<D>>>(c->Id(), ranges.size());
    p->name = name+"placeholder range";
    dag_.AddDagNode(p->plan_id, {}, {c->Id()});
    return c;
  }


  template<typename C1, typename C2, typename M, typename J>
  static auto* mapjoin(C1* c1, C2* c2, M m, J j) {
    // using MsgT = typename decltype(m(*(typename C1::ObjT*)nullptr))::second_type;
    using MsgTFromMap = typename std::remove_pointer<typename function_traits<decltype(m)>::arg1::type>::type::OutputMsgT;
    using MsgT = typename function_traits<decltype(j)>::arg1::type;
    static_assert(std::is_same<MsgT, MsgTFromMap>::value, "...");
    auto *p = plans_.make<MapJoin<C1, C2, typename C1::ObjT, typename C2::ObjT, MsgT>>(c1, c2);
    p->map = m;
    p->join = j;
    dag_.AddDagNode(p->plan_id, {c1->Id()}, {c2->Id()});
    return p;
  }

  template<typename C1, typename C2, typename M, typename J>
  static auto* mappartjoin(C1* c1, C2* c2, M m, J j) {
    // using MsgT = typename decltype(m((TypedPartition<typename C1::ObjT>*)nullptr))::value_type::second_type;
    using MsgTFromMap = typename std::remove_pointer<typename function_traits<decltype(m)>::arg1::type>::type::OutputMsgT;
    using MsgT = typename function_traits<decltype(j)>::arg1::type;
    static_assert(std::is_same<MsgT, MsgTFromMap>::value, "...");
    auto *p = plans_.make<MapPartJoin<C1, C2, typename C1::ObjT, typename C2::ObjT, MsgT>>(c1, c2);
    p->mappart = m;
    p->join = j;
    dag_.AddDagNode(p->plan_id, {c1->Id()}, {c2->Id()});
    return p;
  }

  template<typename C1>
  static void count(C1* c1, std::string name = "") {
    std::string prefix = name + "::count";
    auto *count_collection = placeholder<CountObjT>(1, prefix)->SetName("tmp collection in count");
    mapjoin(c1, count_collection, 
      [](const typename C1::ObjT& obj, Output<int,int>* o) {
        // return std::make_pair(0, 1);
        o->Add(0, 1);
      }, 
      [](CountObjT* a, int b) {
        a->b += b;
      })
    ->SetCombine([](int* a, int b) { *a += b; })
    ->SetName(prefix+"::mapjoin");
    foreach(count_collection, [](const CountObjT& obj) {
      LOG(INFO) << "********** count: " << obj.b << " *********";
    });
    // TODO: remove count_collection
  }

  template<typename C1, typename C2, typename C3, typename M, typename J>
  static auto* mappartwithjoin(C1* c1, C2* c2, C3* c3, M m, J j) {
    // using MsgT = typename decltype(
    //         m((TypedPartition<typename C1::ObjT>*)nullptr, 
    //           (TypedCache<typename C2::ObjT>*)nullptr)
    //         )::value_type::second_type;
    //
    using MsgTFromMap = typename std::remove_pointer<typename function_traits<decltype(m)>::arg2::type>::type::OutputMsgT;
    using MsgT = typename function_traits<decltype(j)>::arg1::type;
    static_assert(std::is_same<MsgT, MsgTFromMap>::value, "...");
    auto *p = plans_.make<MapPartWithJoin<C1, C2, C3, typename C1::ObjT, typename C2::ObjT, typename C3::ObjT, MsgT>>(c1, c2, c3);
    p->mappartwith = m;
    p->join = j;
    dag_.AddDagNode(p->plan_id, {c1->Id(), c2->Id()}, {c3->Id()});
    return p;
  }

  // sort each partition in a collection.
  // this can be used in pagerank which requires each vextex and links
  // objects correspond to each other.
  template<typename C>
  static void sort_each_partition(C* c,
    typename std::enable_if_t<std::is_base_of<Indexable<typename C::ObjT>, typename C::PartT>::value >* = 0) {
    // map c, join c. even though map should not update the partition,
    // we can still update the partition as we are saying we will join c
    // meaning that no other plan can access c. This makes updating in c
    // safe.
    mappartjoin(c, c,
      [](TypedPartition<typename C::ObjT>* p, Output<typename C::ObjT::KeyT, int>* o) {
        auto* indexed_seq_partition = dynamic_cast<Indexable<typename C::ObjT>*>(p);
        CHECK_NOTNULL(indexed_seq_partition);
        indexed_seq_partition->Sort();
      },
      [](typename C::ObjT*, int) {
        // dummy
      })->SetName("sort each part for "+c->Name());
  }

  static auto get_allplans() {
    return plans_.all();
  }
  static auto get_allcollections() {
    return collections_.all();
  }
  static const Dag& get_dag() {
    return dag_;
  }
 private:
  static Store<CollectionBase> collections_;
  static Store<PlanBase> plans_;
  static Dag dag_;
};

} // namespace xyz

