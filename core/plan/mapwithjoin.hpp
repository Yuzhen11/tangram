#pragma once

#include "core/plan/mappartwithjoin.hpp"

namespace xyz {

template<typename T1, typename T2, typename MsgT, typename T3>
struct MapWithJoin : public MapPartWithJoin<T1, T2, MsgT, T3> {
  using MapWithFuncT = std::function<std::pair<typename T2::KeyT, MsgT>(const T1&, TypedCache<T3>*)>;
  using MapVecWithFuncT = std::function<std::vector<std::pair<typename T2::KeyT, MsgT>>(const T1&, TypedCache<T3>*)>;
  using MapPartWithFuncT = 
      std::function<std::shared_ptr<AbstractMapOutput>(std::shared_ptr<AbstractPartition>,
                                                       std::shared_ptr<AbstractPartitionCache>,
                                                       std::shared_ptr<AbstractMapProgressTracker>)>;

  MapWithJoin(int plan_id, Collection<T1> map_collection, 
       Collection<T2> join_collection,
       Collection<T3> with_collection) 
      : MapPartWithJoin<T1, T2, MsgT, T3>(plan_id, map_collection, join_collection, with_collection) {
  }

  void SetMapPartWith() {
    CHECK((mapwith != nullptr) ^ (mapvec_with != nullptr));
    // construct the mappartwith
    this->mappartwith = [this](TypedPartition<T1>* p, 
            AbstractMapProgressTracker* tracker,
            AbstractPartitionCache* cache) {
      std::vector<std::pair<typename T2::KeyT, MsgT>> kvs;
      int i = 0;
      for (auto& elem : *p) {
        if (mapwith != nullptr) {
          kvs.push_back(mapwith(elem, &cache));
        } else {
          auto tmp  = mapvec_with(elem, &cache);  // TODO may be inefficient
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
    this->Register(function_store);
  }

  MapWithFuncT mapwith;  // a (with c) -> b
  MapVecWithFuncT mapvec_with;  // a (with c) -> [b]
};

}  // namespace xyz

