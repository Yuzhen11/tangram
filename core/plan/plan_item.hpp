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
  enum class CombineType {
    Combine, MergeCombine, None
  };
  using MapFuncT = std::function<std::shared_ptr<AbstractMapOutput>(std::shared_ptr<AbstractPartition>)>;
  using JoinFuncT = std::function<void(std::shared_ptr<AbstractPartition>, SArrayBinStream)>;
  using CombineFuncT = std::function<void(std::shared_ptr<AbstractMapOutput>)>;
  using MergeCombinedFuncT = std::function<SArrayBinStream(const std::vector<std::shared_ptr<AbstractMapOutput>>& map_outputs, int part_id)>;

  PlanItem(int _plan_id, int _map_collection_id, int _join_collection_id)
      :plan_id(_plan_id), map_collection_id(_map_collection_id), join_collection_id(_join_collection_id) {}

  MapFuncT map;
  JoinFuncT join;
  CombineFuncT combine;
  MergeCombinedFuncT merge_combine;

  int plan_id;
  int map_collection_id;
  int join_collection_id;
  CombineType combine_type;
};

}  // namespace xyz

