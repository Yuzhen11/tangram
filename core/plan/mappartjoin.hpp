#pragma once

#include "core/plan/plan_base.hpp"

#include "core/map_output/partitioned_map_output.hpp"
#include "core/index/abstract_key_to_part_mapper.hpp"

#include "core/plan/abstract_function_store.hpp"
#include "core/plan/plan_spec.hpp"
#include "core/plan/join_helper.hpp"

namespace xyz {

template<typename C1, typename C2, typename ObjT1, typename ObjT2, typename MsgT>
struct MapPartJoin;

template<typename MsgT, typename C1, typename C2>
MapPartJoin<C1, C2, typename C1::ObjT, typename C2::ObjT, MsgT> GetMapPartJoin(int plan_id, C1* c1, C2* c2) {
  MapPartJoin<C1, C2, typename C1::ObjT, typename C2::ObjT, MsgT> plan(plan_id, c1, c2);
  return plan;
}

template<typename C1, typename C2, typename ObjT1, typename ObjT2, typename MsgT>
struct MapPartJoin : public PlanBase {
  using MapPartFuncT = std::function<std::vector<std::pair<typename ObjT2::KeyT, MsgT>>(
          TypedPartition<ObjT1>* p, AbstractMapProgressTracker* t)>;
  using JoinFuncT = std::function<void(ObjT2*, const MsgT&)>;
  using CombineFuncT = std::function<MsgT(const MsgT&, const MsgT&)>;

  // for internal use
  using MapFuncTempT = std::function<std::shared_ptr<AbstractMapOutput>(
          std::shared_ptr<AbstractPartition>, std::shared_ptr<AbstractMapProgressTracker>)>;

  MapPartJoin(int _plan_id, C1* _map_collection, C2* _join_collection)
      : PlanBase(_plan_id), map_collection(_map_collection), join_collection(_join_collection) {}

  virtual PlanSpec GetPlanSpec() override {
    PlanSpec plan;
    plan.plan_id = plan_id;
    plan.map_collection_id = map_collection->Id();
    plan.join_collection_id = join_collection->Id();
    plan.num_iter = num_iter;
    return plan;
  }

  virtual void Register(std::shared_ptr<AbstractFunctionStore> function_store) override {
    auto map_part = GetMapPartFunc();
    function_store->AddPartToIntermediate(plan_id, [this, map_part](
                std::shared_ptr<AbstractPartition> partition,
                std::shared_ptr<AbstractMapProgressTracker> tracker) {
      auto map_output = map_part(partition, tracker);
      if (combine) {
        static_cast<TypedMapOutput<typename ObjT2::KeyT, MsgT>*>(map_output.get())->SetCombineFunc(combine);
        map_output->Combine();
      }
      return map_output;
    });

    CHECK_NOTNULL(join);
    function_store->AddJoinFunc(plan_id, GetJoinPartFunc<ObjT2, MsgT>(join));
  }

  MapFuncTempT GetMapPartFunc() {
    CHECK_NOTNULL(mappart);
    return [this](std::shared_ptr<AbstractPartition> partition, std::shared_ptr<AbstractMapProgressTracker> tracker) {
      auto* p = static_cast<TypedPartition<ObjT1>*>(partition.get());
      CHECK_NOTNULL(join_collection->GetMapper());
      auto output = std::make_shared<PartitionedMapOutput<typename ObjT2::KeyT, MsgT>>(join_collection->GetMapper());
      CHECK_NOTNULL(p);
      CHECK_NOTNULL(output);
      output->Add(mappart(p, tracker.get()));
      return output;
    };
  }

  C1* map_collection;
  C2* join_collection;
  //int num_iter = 1;

  MapPartFuncT mappart;
  JoinFuncT join;
  CombineFuncT combine;
};

}  // namespace xyz

