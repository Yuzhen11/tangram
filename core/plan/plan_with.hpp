#pragma once

#include "core/plan/plan.hpp"
#include "core/cache/typed_cache.hpp"

namespace xyz {

template<typename T1, typename T2, typename MsgT, typename T3>
class PlanWith : public Plan<T1, T2, MsgT> {
 public:
  using MapWithFuncT = std::function<std::pair<typename T2::KeyT, MsgT>(const T1&, TypedCache<T3>*)>;
  using MapPartWithFuncT = 
      std::function<std::shared_ptr<AbstractMapOutput>(std::shared_ptr<AbstractPartition>,
                                                       std::shared_ptr<AbstractPartitionCache>,
                                                       std::shared_ptr<AbstractMapProgressTracker>)>;

  PlanWith(int plan_id, Collection<T1> map_collection, 
       Collection<T2> join_collection,
       Collection<T3> with_collection) 
      : Plan<T1, T2, MsgT>(plan_id, map_collection, join_collection),
        with_collection_(with_collection) {
  }

  void RegisterPlanWith(std::shared_ptr<AbstractFunctionStore> function_store) {
    auto map_part_with = GetMapPartWithFunc();
    function_store->AddMapWith(this->plan_id, [this, map_part_with](
                std::shared_ptr<AbstractPartition> partition,
                std::shared_ptr<AbstractPartitionCache> partition_cache,
                std::shared_ptr<AbstractMapProgressTracker> tracker) {
      auto map_output = map_part_with(partition, partition_cache, tracker);
      if (this->combine) {
        static_cast<TypedMapOutput<typename T2::KeyT, MsgT>*>(map_output.get())->SetCombineFunc(this->combine);
        map_output->Combine();
      }
      return map_output;
    });
  }

  MapPartWithFuncT GetMapPartWithFunc() {
    CHECK_NOTNULL(mapwith);
    return [this](std::shared_ptr<AbstractPartition> partition, 
              std::shared_ptr<AbstractPartitionCache> cache,
              std::shared_ptr<AbstractMapProgressTracker> tracker) {
      // TODO: Fix the version
      int version = 0;
      TypedCache<T3> typed_cache(cache, with_collection_.mapper, with_collection_.id, version);
      auto* p = static_cast<TypedPartition<T1>*>(partition.get());
      CHECK_NOTNULL(this->join_collection.mapper);
      auto output = std::make_shared<PartitionedMapOutput<typename T2::KeyT, MsgT>>(this->join_collection.mapper);
      CHECK_NOTNULL(p);
      CHECK_NOTNULL(output);
      int i = 0;
      for (auto& elem : *p) {
        output->Add(mapwith(elem, typed_cache));
        i += 1;
        if (i % 10 == 0) {
          tracker->Report(i);
        }
      }
      return output;
    };
  }

  Collection<T3> with_collection_;
  MapWithFuncT mapwith;
};

}  // namespace xyz

