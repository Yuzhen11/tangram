#pragma once

#include "core/collection.hpp"
#include "core/output_manager.hpp"

#include "core/plan_item.hpp"
#include "core/partition.hpp"

namespace xyz {

template<typename T1, typename T2, typename MsgT>
class Plan {
 public:
  Plan(int plan_id, Collection<T1> map_collection, Collection<T2> join_collection)
      :plan_id_(plan_id), map_collection_(map_collection), join_collection_(join_collection) {}

  void SetMap(const std::function<std::pair<typename T2::KeyT, MsgT>(T1)>& l) {
    map_ = l;
  }

  void SetJoin(const std::function<T1(T1, MsgT)>& l) {
    join_ = l;
  }

  PlanItem GetPlanItem() {
    PlanItem::MapFuncT map = [this](std::shared_ptr<AbstractPartition> _partition, std::shared_ptr<AbstractOutputManager> _output) {
      auto* p = static_cast<Partition<T1>*>(_partition.get());
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
  std::function<std::pair<typename T2::KeyT, MsgT>(T1)> map_;
  std::function<T1(T1, MsgT)> join_;
};

}  // namespace
