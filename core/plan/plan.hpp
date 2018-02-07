#pragma once

#include "core/collection.hpp"
#include "core/map_output/abstract_map_output.hpp"

#include "core/partition/abstract_partition.hpp"

#include "core/map_output/partitioned_map_output_helper.hpp"
#include "core/map_output/partitioned_map_output.hpp"

#include "core/index/hash_key_to_part_mapper.hpp"

#include "core/plan/abstract_function_store.hpp"
#include "core/plan/plan_spec.hpp"


namespace xyz {

/*
 * Requires T2 to be in the form {T2::KeyT, T2::ValT}
 */
template<typename T1, typename T2, typename MsgT>
class Plan {
 public:
  using MapFuncT = std::function<std::pair<typename T2::KeyT, MsgT>(const T1&)>;
  // For most of the cases, T2::ValT == MsgT
  using JoinFuncT = std::function<typename T2::ValT(const typename T2::ValT&, const MsgT&)>;
  using CombineFuncT = std::function<MsgT(const MsgT&, const MsgT&)>;

  using MapPartFuncT = std::function<std::shared_ptr<AbstractMapOutput>(std::shared_ptr<AbstractPartition>)>;

  Plan(int _plan_id, Collection<T1> _map_collection, Collection<T2> _join_collection)
      :plan_id(_plan_id), map_collection(_map_collection), join_collection(_join_collection),
    plan_spec(plan_id, map_collection.id, join_collection.id, -1) {
  }

  void Register(std::shared_ptr<AbstractFunctionStore> function_store) {
    auto map_part = GetMapPartFunc();
    function_store->AddPartToIntermediate(plan_id, [this, map_part](std::shared_ptr<AbstractPartition> partition) {
      auto map_output = map_part(partition);
      if (combine) {
        static_cast<TypedMapOutput<typename T2::KeyT, MsgT>*>(map_output->get())->SetCombineFunc(combine);
        map_output->Combine();
      }
      return map_output;
    });
  }

  void RegisterShuffleCombine(std::shared_ptr<AbstractFunctionStore> function_store) {
    auto map_part = GetMapPartFunc();
    // part -> mapoutput_manager
    function_store->AddPartToOutputManager(plan_id, [this, map_part](std::shared_ptr<AbstractPartition> partition) {
      auto map_output = map_part(partition);
      if (combine) {
        static_cast<TypedMapOutput<typename T2::KeyT, MsgT>*>(map_output->get())->SetCombineFunc(combine);
      }
      return map_output;
    });
    // mapoutput_manager -> bin
    function_store->AddOutputsToBin(plan_id, [](const std::vector<std::shared_ptr<AbstractMapOutput>>& map_outputs, int part_id) {
      return MergeCombineMultipleMapOutput<typename T2::KeyT, MsgT>(map_outputs, part_id);
    });
  }
  PlanSpec GetPlanSpec() {
    return plan_spec;
  }

  MapPartFuncT GetMapPartFunc() {
    CHECK(map != nullptr);
    return [this](std::shared_ptr<AbstractPartition> partition) {
      auto* p = static_cast<TypedPartition<T1>*>(partition.get());
      CHECK_NOTNULL(join_collection.mapper);
      auto output = std::make_shared<PartitionedMapOutput<typename T2::KeyT, MsgT>>(join_collection.mapper);
      CHECK_NOTNULL(p);
      CHECK_NOTNULL(output);
      for (auto& elem : *p) {
        output->Add(map(elem));
      }
      return output;
    };
  }

  int plan_id;
  Collection<T1> map_collection;
  Collection<T2> join_collection;
  PlanSpec plan_spec;

  MapFuncT map;
  JoinFuncT join;
  CombineFuncT combine;
};

}  // namespace xyz
