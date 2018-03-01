#pragma once

#include "core/cache/typed_cache.hpp"

#include "core/collection.hpp"

#include "core/partition/abstract_partition.hpp"
#include "core/map_output/partitioned_map_output.hpp"
#include "core/index/abstract_key_to_part_mapper.hpp"

#include "core/plan/abstract_function_store.hpp"
#include "core/plan/plan_spec.hpp"
#include "core/plan/join_helper.hpp"

namespace xyz {

template<typename T1, typename T2, typename MsgT, typename T3>
struct MapPartWithJoin {
  using MapPartWithFuncT = std::function<std::vector<std::pair<typename T2::KeyT, MsgT>>(
          TypedPartition<T1>* p, 
          AbstractMapProgressTracker* t,
          AbstractPartitionCache* c)>;
  using JoinFuncT = std::function<void(T2*, const MsgT&)>;
  using CombineFuncT = std::function<MsgT(const MsgT&, const MsgT&)>;

  // for internal use
  using MapPartWithTempFuncT= 
      std::function<std::shared_ptr<AbstractMapOutput>(std::shared_ptr<AbstractPartition>,
                                                       std::shared_ptr<AbstractPartitionCache>,
                                                       std::shared_ptr<AbstractMapProgressTracker>)>;

  MapPartWithJoin(int _plan_id, Collection<T1> _map_collection, 
          Collection<T2> _join_collection, Collection<T3> _with_collection)
      :plan_id(_plan_id), map_collection(_map_collection), 
       join_collection(_join_collection), with_collection(_with_collection) {
  }

  PlanSpec GetPlanSpec() {
    PlanSpec plan;
    plan.plan_id = plan_id;
    plan.map_collection_id = map_collection.id;
    plan.join_collection_id = join_collection.id;
    plan.with_collection_id = with_collection.id;
    plan.num_iter = num_iter;
    return plan;
  }

  void Register(std::shared_ptr<AbstractFunctionStore> function_store) {
    auto map_part_with = GetMapPartWithFunc();
    function_store->AddMapWith(this->plan_id, [this, map_part_with](
                std::shared_ptr<AbstractPartition> partition,
                std::shared_ptr<AbstractPartitionCache> partition_cache,
                std::shared_ptr<AbstractMapProgressTracker> tracker) {
      auto map_output = map_part_with(partition, tracker, partition_cache);
      if (this->combine) {
        static_cast<TypedMapOutput<typename T2::KeyT, MsgT>*>(map_output.get())->SetCombineFunc(this->combine);
        map_output->Combine();
      }
      return map_output;
    });

    CHECK_NOTNULL(join);
    function_store->AddJoinFunc(plan_id, GetJoinPartFunc<T2, MsgT>(join));
  }

  MapPartWithTempFuncT GetMapPartWithFunc() {
    CHECK_NOTNULL(mappartwith);
    return [this](std::shared_ptr<AbstractPartition> partition, 
              std::shared_ptr<AbstractPartitionCache> cache,
              std::shared_ptr<AbstractMapProgressTracker> tracker) {
      // TODO: Fix the version
      int version = 0;
      TypedCache<T3> typed_cache(cache, with_collection.mapper, with_collection.id, version);
      auto* p = static_cast<TypedPartition<T1>*>(partition.get());
      CHECK_NOTNULL(this->join_collection.mapper);
      auto output = std::make_shared<PartitionedMapOutput<typename T2::KeyT, MsgT>>(this->join_collection.mapper);
      CHECK_NOTNULL(p);
      CHECK_NOTNULL(output);
      output->Add(mappartwith(p, tracker.get(), &typed_cache));
      return output;
    };
  }

  int plan_id;
  Collection<T1> map_collection;
  Collection<T2> join_collection;
  Collection<T3> with_collection;
  int num_iter = 1;

  MapPartWithFuncT mappartwith;
  JoinFuncT join;
};

}  // namespace xyz

