#pragma once

#include "core/collection.hpp"
#include "core/map_output/abstract_map_output.hpp"

#include "core/plan/plan_item.hpp"
#include "core/partition/abstract_partition.hpp"

#include "core/map_output/partitioned_map_output_helper.hpp"
#include "core/map_output/partitioned_map_output.hpp"

#include "core/index/hash_key_to_part_mapper.hpp"

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

  Plan(int plan_id, Collection<T1> map_collection, Collection<T2> join_collection)
      :plan_id_(plan_id), map_collection_(map_collection), join_collection_(join_collection) {}

  void SetMap(const MapFuncT& func) {
    map_ = func;
  }

  void SetJoin(const JoinFuncT& func) {
    join_ = func;
  }
  
  void SetCombine(const CombineFuncT& func) {
    combine_ = func;
  }

  PlanItem GetPlanItem() {
    PlanItem plan_item(plan_id_, map_collection_.id, join_collection_.id);
    // Set the map func.
    plan_item.map = [this](std::shared_ptr<AbstractPartition> partition) {
      auto* p = static_cast<TypedPartition<T1>*>(partition.get());
      CHECK_NOTNULL(join_collection_.mapper);
      auto output = std::make_shared<PartitionedMapOutput<typename T2::KeyT, MsgT>>(join_collection_.mapper);
      CHECK_NOTNULL(p);
      CHECK_NOTNULL(output);
      for (auto& elem : *p) {
        output->Add(map_(elem));
      }
      return output;
    };
    // Set the combine func.
    plan_item.combine = [this](std::shared_ptr<AbstractMapOutput> map_output) {
      map_output->Combine();
    };
    // Set the merge_combine func. Wrap the type inside.
    plan_item.merge_combine = [this](const std::vector<std::shared_ptr<AbstractMapOutput>>& map_outputs, int part_id) {
      return MergeCombineMultipleMapOutput<typename T2::KeyT, MsgT>(map_outputs, part_id);
    };
    // Set the join func.
    plan_item.join = [this](std::shared_ptr<AbstractPartition> partition, SArrayBinStream bin) {
      // TODO
    };
    return plan_item;
  }

 private:
  int plan_id_;
  Collection<T1> map_collection_;
  Collection<T2> join_collection_;
  MapFuncT map_;
  JoinFuncT join_;
  CombineFuncT combine_;
};

}  // namespace xyz
