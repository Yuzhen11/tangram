#pragma once

#include "core/collection.hpp"
#include "core/map_output/abstract_map_output.hpp"

#include "core/partition/abstract_partition.hpp"

#include "core/map_output/partitioned_map_output.hpp"

#include "core/index/hash_key_to_part_mapper.hpp"

#include "core/plan/abstract_function_store.hpp"
#include "core/plan/plan_spec.hpp"

#include "core/plan/mappartjoin.hpp"

namespace xyz {

/*
 * Requires T2 to be in the form {T2::KeyT, T2::ValT}
 */
template<typename T1, typename T2, typename MsgT>
struct MapJoin : public MapPartJoin<T1, T2, MsgT>{
  using MapFuncT = std::function<std::pair<typename T2::KeyT, MsgT>(const T1&)>;
  using MapVecFuncT = std::function<std::vector<std::pair<typename T2::KeyT, MsgT>>(const T1&)>;

  MapJoin(int plan_id, Collection<T1> map_collection, Collection<T2> join_collection)
      : MapPartJoin<T1, T2, MsgT>(plan_id, map_collection, join_collection) {
  }

  void SetMapPart() {
    CHECK((map != nullptr) ^ (map_vec != nullptr));
    // construct the mappart
    this->mappart = [this](TypedPartition<T1>* p, AbstractMapProgressTracker* tracker) {
      CHECK_NOTNULL(p);
      std::vector<std::pair<typename T2::KeyT, MsgT>> kvs;
      int i = 0;
      for (auto& elem : *p) {
        if (map != nullptr) {
          kvs.push_back(map(elem));
        } else {
          auto tmp  = map_vec(elem);  // TODO may be inefficient
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
    SetMapPart();
    MapPartJoin<T1, T2, MsgT>::Register(function_store);
  }

  MapFuncT map;  // a -> b
  MapVecFuncT map_vec;  // a -> [b] 

};

}  // namespace xyz
