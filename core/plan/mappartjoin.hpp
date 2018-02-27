#pragma once

#include "core/collection.hpp"

#include "core/partition/abstract_partition.hpp"
#include "core/map_output/partitioned_map_output.hpp"
#include "core/index/abstract_key_to_part_mapper.hpp"

#include "core/plan/abstract_function_store.hpp"
#include "core/plan/plan_spec.hpp"
#include "core/plan/join_helper.hpp"

namespace xyz {

template<typename T1, typename T2, typename MsgT>
struct MapPartJoin {
  using MapPartFuncT = std::function<std::vector<std::pair<typename T2::KeyT, MsgT>>(
          TypedPartition<T1>* p, AbstractMapProgressTracker* t)>;
  using JoinFuncT = std::function<void(T2*, const MsgT&)>;
  using CombineFuncT = std::function<MsgT(const MsgT&, const MsgT&)>;

  // for internal use
  using MapFuncTempT = std::function<std::shared_ptr<AbstractMapOutput>(
          std::shared_ptr<AbstractPartition>, std::shared_ptr<AbstractMapProgressTracker>)>;

  MapPartJoin(int _plan_id, Collection<T1> _map_collection, Collection<T2> _join_collection)
      :plan_id(_plan_id), map_collection(_map_collection), join_collection(_join_collection) {
  }

  PlanSpec GetPlanSpec() {
    PlanSpec plan;
    plan.plan_id = plan_id;
    plan.map_collection_id = map_collection.id;
    plan.join_collection_id = join_collection.id;
    return plan;
  }

  void Register(std::shared_ptr<AbstractFunctionStore> function_store) {
    auto map_part = GetMapPartFunc();
    function_store->AddPartToIntermediate(plan_id, [this, map_part](
                std::shared_ptr<AbstractPartition> partition,
                std::shared_ptr<AbstractMapProgressTracker> tracker) {
      auto map_output = map_part(partition, tracker);
      if (combine) {
        static_cast<TypedMapOutput<typename T2::KeyT, MsgT>*>(map_output.get())->SetCombineFunc(combine);
        map_output->Combine();
      }
      return map_output;
    });

    CHECK_NOTNULL(join);
    function_store->AddJoinFunc(plan_id, GetJoinPartFunc<T2, MsgT>(join));
  }

  MapFuncTempT GetMapPartFunc() {
    CHECK_NOTNULL(mappart);
    return [this](std::shared_ptr<AbstractPartition> partition, std::shared_ptr<AbstractMapProgressTracker> tracker) {
      auto* p = static_cast<TypedPartition<T1>*>(partition.get());
      CHECK_NOTNULL(join_collection.mapper);
      auto output = std::make_shared<PartitionedMapOutput<typename T2::KeyT, MsgT>>(join_collection.mapper);
      CHECK_NOTNULL(p);
      CHECK_NOTNULL(output);
      output->Add(mappart(p, tracker.get()));
      return output;
    };
  }

  int plan_id;
  Collection<T1> map_collection;
  Collection<T2> join_collection;

  MapPartFuncT mappart;
  JoinFuncT join;
  CombineFuncT combine;
};

}  // namespace xyz

