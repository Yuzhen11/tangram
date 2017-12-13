#pragma once

#include <functional>

#include "core/abstract_partition.hpp"
#include "core/abstract_output_manager.hpp"

namespace xyz {

/*
 * A wrapper class for Map and Join function in the actual plan,
 * so that all type information is hidden by the PlanItem.
 */
class PlanItem {
 public:
  using MapFuncT = std::function<void(std::shared_ptr<AbstractPartition>, AbstractOutputManager*)>;
  using JoinFuncT = std::function<void(std::shared_ptr<AbstractPartition>)>;

  PlanItem(MapFuncT map, JoinFuncT join):map_(map), join_(join) {}

  const MapFuncT& GetMap() const {
    return map_;
  }
  const JoinFuncT& GetJoin() const {
    return join_;
  }
  
 private:
  MapFuncT map_;
  JoinFuncT join_;
};

}  // namespace

