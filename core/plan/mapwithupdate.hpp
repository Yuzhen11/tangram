#pragma once

#include "core/plan/mappartwithupdate.hpp"

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
  using MapWithFuncT = std::function<void(const ObjT1&, TypedCache<ObjT3>*, Output<typename ObjT3::KeyT, MsgT>*)>;

  MapWithJoin(int plan_id, C1* map_collection, 
       C2* with_collection,
       C3* update_collection) 
      : MapPartWithJoin<C1,C2,C3,ObjT1,ObjT2,ObjT3,MsgT>(plan_id, map_collection, with_collection, update_collection) {
  }

  void SetMapPartWith() {
    CHECK(mapwith != nullptr);
    // construct the mappartwith
    this->mappartwith = [this](TypedPartition<ObjT1>* p, 
            TypedCache<ObjT2>* typed_cache,
            Output<typename ObjT3::KeyT, MsgT>* o) {
      int i = 0;
      for (auto& elem : *p) {
        mapwith(elem, typed_cache, o);
        i += 1;
      }
    };
  }

  void Register(std::shared_ptr<AbstractFunctionStore> function_store) {
    SetMapPartWith();
    MapPartWithJoin<C1,C2,C3,ObjT1,ObjT2,ObjT3,MsgT>::Register(function_store);
  }

  MapWithFuncT mapwith;  // a (with c) -> b
};

}  // namespace xyz

