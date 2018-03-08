#pragma once

#include "core/plan/mappartwithjoin.hpp"

namespace xyz {

template<typename C1, typename C2, typename C3, typename ObjT1, typename ObjT2, typename ObjT3, typename MsgT>
struct MapWithJoin;

template<typename MsgT, typename C1, typename C2, typename C3>
MapWithJoin<C1, C2, C3, typename C1::ObjT, typename C2::ObjT, typename C3::ObjT, MsgT> GetMapWithJoin(int plan_id, C1* c1, C2* c2, C3* c3) {
  MapWithJoin<C1, C2, C3, typename C1::ObjT, typename C2::ObjT, typename C3::ObjT, MsgT> plan(plan_id, c1, c2, c3);
  return plan;
}

template<typename C1, typename C2, typename C3, typename ObjT1, typename ObjT2, typename ObjT3, typename MsgT>
struct MapWithJoin : public MapPartWithJoin<C1,C2,C3,ObjT1,ObjT2,ObjT3,MsgT> {
  using MapWithFuncT = std::function<std::pair<typename ObjT2::KeyT, MsgT>(const ObjT1&, TypedCache<ObjT3>*)>;
  using MapVecWithFuncT = std::function<std::vector<std::pair<typename ObjT2::KeyT, MsgT>>(const ObjT1&, TypedCache<ObjT3>*)>;

  MapWithJoin(int plan_id, C1* map_collection, 
       C2* with_collection,
       C3* join_collection) 
      : MapPartWithJoin<C1,C2,C3,ObjT1,ObjT2,ObjT3,MsgT>(plan_id, map_collection, with_collection, join_collection) {
  }

  void SetMapPartWith() {
    CHECK((mapwith != nullptr) ^ (mapvec_with != nullptr));
    // construct the mappartwith
    this->mappartwith = [this](TypedPartition<ObjT1>* p, 
            TypedCache<ObjT2>* typed_cache,
            AbstractMapProgressTracker* tracker) {
      std::vector<std::pair<typename ObjT2::KeyT, MsgT>> kvs;
      int i = 0;
      for (auto& elem : *p) {
        if (mapwith != nullptr) {
          kvs.push_back(mapwith(elem, typed_cache));
        } else {
          auto tmp  = mapvec_with(elem, typed_cache);  // TODO may be inefficient
          kvs.insert(kvs.end(), tmp.begin(), tmp.end());
        }
        i += 1;
        if (i % 10 == 0) {
          tracker->Report(i);
        }
      }
      return kvs;
    };
  }

  void Register(std::shared_ptr<AbstractFunctionStore> function_store) {
    SetMapPartWith();
    MapPartWithJoin<C1,C2,C3,ObjT1,ObjT2,ObjT3,MsgT>::Register(function_store);
  }

  MapWithFuncT mapwith;  // a (with c) -> b
  MapVecWithFuncT mapvec_with;  // a (with c) -> [b]
};

}  // namespace xyz

