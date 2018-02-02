#pragma once

#include "core/plan/plan.hpp"

namespace xyz {

template<typename T1, typename T2, typename MsgT, typename T3>
class PlanWith : public Plan<T1, T2, MsgT> {
 public:
  // using MapWithFuncT = std::function<std::pair<typename T2::KeyT, MsgT>(const T1&, TypedObjCache<T3>*)>;

  PlanWith(int plan_id, Collection<T1> map_collection, 
       Collection<T2> join_collection,
       Collection<T3> with_collection) 
      : Plan<T1, T2, MsgT>(plan_id, map_collection, join_collection),
        with_collection_(with_collection) {
  }

  Collection<T3> with_collection_;
};

}  // namespace xyz

