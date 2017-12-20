#pragma once

#include <functional>
#include <memory>

#include "core/partition/abstract_partition.hpp"
#include "core/map_output/abstract_map_output.hpp"

namespace xyz {

/*
 * A wrapper class for Map and Join function in the actual plan,
 * so that all type information is hidden by the PlanItem.
 */
class PlanItem {
 public:
  using MapFuncT = std::function<void(std::shared_ptr<AbstractPartition>, std::shared_ptr<AbstractMapOutput>)>;
  using JoinFuncT = std::function<void(std::shared_ptr<AbstractPartition>)>;

  PlanItem(int _plan_id, int _map_collection_id, int _join_collection_id, 
          MapFuncT _map, JoinFuncT _join)
      :plan_id(_plan_id), map_collection_id(_map_collection_id), join_collection_id(_join_collection_id),
       map(_map), join(_join) {}

  MapFuncT map;
  JoinFuncT join;
  int plan_id;
  int map_collection_id;
  int join_collection_id;
};

}  // namespace

