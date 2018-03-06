#pragma once

#include "core/plan/plan_base.hpp"

#include "core/cache/typed_cache.hpp"

#include "core/plan/collection.hpp"

#include "core/partition/abstract_partition.hpp"
#include "core/map_output/partitioned_map_output.hpp"
#include "core/index/abstract_key_to_part_mapper.hpp"

#include "core/plan/abstract_function_store.hpp"
#include "core/plan/plan_spec.hpp"
#include "core/plan/join_helper.hpp"

namespace xyz {

template<typename C1, typename C2, typename C3, typename ObjT1, typename ObjT2, typename ObjT3, typename MsgT>
struct MapPartWithJoin;

template<typename MsgT, typename C1, typename C2, typename C3>
MapPartWithJoin<C1, C2, C3, typename C1::ObjT, typename C2::ObjT, typename C3::ObjT, MsgT> GetMapPartWithJoin(int plan_id, C1* c1, C2* c2, C3* c3) {
  MapPartWithJoin<C1, C2, C3, typename C1::ObjT, typename C2::ObjT, typename C3::ObjT, MsgT> plan(plan_id, c1, c2, c3);
  return plan;
}

template<typename C1, typename C2, typename C3, typename ObjT1, typename ObjT2, typename ObjT3, typename MsgT>
struct MapPartWithJoin : public PlanBase {
  using MapPartWithFuncT = std::function<std::vector<std::pair<typename ObjT2::KeyT, MsgT>>(
          TypedPartition<ObjT1>* p, 
          AbstractMapProgressTracker* t,
          TypedCache<ObjT3>* c)>;
  using JoinFuncT = std::function<void(ObjT2*, const MsgT&)>;
  using CombineFuncT = std::function<MsgT(const MsgT&, const MsgT&)>;

  // for internal use
  using MapPartWithTempFuncT= 
      std::function<std::shared_ptr<AbstractMapOutput>(std::shared_ptr<AbstractPartition>,
                                                       std::shared_ptr<AbstractPartitionCache>,
                                                       std::shared_ptr<AbstractMapProgressTracker>)>;

  MapPartWithJoin(int _plan_id, C1* _map_collection, 
          C2* _join_collection, C3* _with_collection)
      : PlanBase(_plan_id), map_collection(_map_collection), 
       join_collection(_join_collection), with_collection(_with_collection) {
  }

  virtual PlanSpec GetPlanSpec() override {
    PlanSpec plan;
    plan.plan_id = plan_id;
    plan.map_collection_id = map_collection->Id();
    plan.join_collection_id = join_collection->Id();
    plan.with_collection_id = with_collection->Id();
    plan.num_iter = num_iter;
    return plan;
  }

  virtual void Register(std::shared_ptr<AbstractFunctionStore> function_store) override {
    auto map_part_with = GetMapPartWithFunc();
    function_store->AddMapWith(this->plan_id, [this, map_part_with](
                std::shared_ptr<AbstractPartition> partition,
                std::shared_ptr<AbstractPartitionCache> partition_cache,
                std::shared_ptr<AbstractMapProgressTracker> tracker) {
      auto map_output = map_part_with(partition, partition_cache, tracker);
      if (this->combine) {
        static_cast<TypedMapOutput<typename ObjT2::KeyT, MsgT>*>(map_output.get())->SetCombineFunc(this->combine);
        map_output->Combine();
      }
      return map_output;
    });

    CHECK_NOTNULL(join);
    function_store->AddJoinFunc(plan_id, GetJoinPartFunc<ObjT2, MsgT>(join));
  }

  MapPartWithTempFuncT GetMapPartWithFunc() {
    CHECK_NOTNULL(mappartwith);
    return [this](std::shared_ptr<AbstractPartition> partition, 
              std::shared_ptr<AbstractPartitionCache> cache,
              std::shared_ptr<AbstractMapProgressTracker> tracker) {
      // TODO: Fix the version
      int version = 0;
      TypedCache<ObjT3> typed_cache(cache, with_collection->GetMapper(), with_collection->Id(), version);
      auto* p = static_cast<TypedPartition<ObjT1>*>(partition.get());
      CHECK_NOTNULL(this->join_collection->GetMapper());
      auto output = std::make_shared<PartitionedMapOutput<typename ObjT2::KeyT, MsgT>>(this->join_collection->GetMapper());
      CHECK_NOTNULL(p);
      CHECK_NOTNULL(output);
      output->Add(mappartwith(p, tracker.get(), &typed_cache));
      return output;
    };
  }

  C1* map_collection;
  C2* join_collection;
  C3* with_collection;
  //int num_iter = 1;

  MapPartWithFuncT mappartwith;
  JoinFuncT join;
  CombineFuncT combine;
};

}  // namespace xyz

