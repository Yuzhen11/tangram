#pragma once

#include "core/plan/collection.hpp"
#include "core/map_output/abstract_map_output.hpp"

#include "core/partition/abstract_partition.hpp"

#include "core/map_output/partitioned_map_output.hpp"

#include "core/index/hash_key_to_part_mapper.hpp"
#include "core/index/range_key_to_part_mapper.hpp"

#include "core/plan/abstract_function_store.hpp"
#include "core/plan/plan_spec.hpp"

#include "core/plan/mappartjoin.hpp"

namespace xyz {

template<typename C1, typename C2, typename ObjT1, typename ObjT2, typename MsgT>
struct MapJoin;

template<typename MsgT, typename C1, typename C2>
MapJoin<C1, C2, typename C1::ObjT, typename C2::ObjT, MsgT> GetMapJoin(int plan_id, C1* c1, C2* c2) {
  MapJoin<C1, C2, typename C1::ObjT, typename C2::ObjT, MsgT> plan(plan_id, c1, c2);
  return plan;
}

/*
 * Requires T2 to be in the form {T2::KeyT, T2::ValT}
 */
template<typename C1, typename C2, typename ObjT1, typename ObjT2, typename MsgT>
struct MapJoin : public MapPartJoin<C1, C2, ObjT1, ObjT2, MsgT>{
  using MapFuncT = std::function<void(const ObjT1&, Output<typename ObjT2::KeyT, MsgT>*)>;

  MapJoin(int plan_id, C1* map_collection, C2* join_collection)
      : MapPartJoin<C1, C2, ObjT1, ObjT2, MsgT>(plan_id, map_collection, join_collection) {
  }

  void SetMapPart() {
    CHECK(map != nullptr);
    // construct the mappart
    this->mappart = [this](TypedPartition<ObjT1>* p, 
        Output<typename ObjT2::KeyT, MsgT>* o) {
      CHECK_NOTNULL(p);
      int i = 0;
      for (auto& elem : *p) {
        map(elem, o);
        i += 1;
      }
    };
  }
  void Register(std::shared_ptr<AbstractFunctionStore> function_store) {
    SetMapPart();
    MapPartJoin<C1, C2, ObjT1, ObjT2, MsgT>::Register(function_store);
  }

  MapFuncT map;  // a -> b
};

}  // namespace xyz
