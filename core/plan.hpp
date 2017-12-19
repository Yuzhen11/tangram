#pragma once

#include "core/collection.hpp"
#include "core/output_manager.hpp"

#include "core/plan_item.hpp"
#include "core/abstract_partition.hpp"

namespace xyz {

/*
 * This class depends on the real implementation of OutputManager and Partition.
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
    PlanItem::MapFuncT map = [this](std::shared_ptr<AbstractPartition> _partition, std::shared_ptr<AbstractOutputManager> _output) {
      auto* p = static_cast<TypedPartition<T1>*>(_partition.get());
      auto* output = static_cast<OutputManager<typename T2::KeyT, MsgT>*>(_output.get());
      CHECK_NOTNULL(p);
      CHECK_NOTNULL(output);
      for (auto& elem : *p) {
        output->Add(map_(elem));
      }
    };
    PlanItem::JoinFuncT join = [this](std::shared_ptr<AbstractPartition> partition) {
    };
    return PlanItem(plan_id_, map_collection_.id, join_collection_.id, map, join);
  }

 private:
  int plan_id_;
  Collection<T1> map_collection_;
  Collection<T2> join_collection_;
  MapFuncT map_;
  JoinFuncT join_;
  CombineFuncT combine_;
};

}  // namespace
