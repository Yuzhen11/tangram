#pragma once

#include <map>

#include "core/plan/plan_item.hpp"
#include "core/intermediate/abstract_intermediate_store.hpp"

#include "core/map_output/abstract_map_output.hpp"
#include "core/map_output/map_output_storage.hpp"

namespace xyz { 

/*
 * Store all the functions.
 */
class FunctionStore {
 public:
  void AddPlanItem(PlanItem plan);
  // Partition -> MapOutputManager
  using MapToMapOutputManagerFuncT = std::function<void(std::shared_ptr<AbstractPartition>, 
                                                        std::shared_ptr<MapOutputManager>)>;
  // MapOutput -> IntermediateStore
  using MapOutputToIntermediateStoreFuncT = std::function<void(const std::vector<std::shared_ptr<AbstractMapOutput>>&, 
                                                               std::shared_ptr<AbstractIntermediateStore>,
                                                               int part_id)>;
  // Partition -> IntermediateStore
  using MapToIntermediateStoreFuncT = std::function<void(std::shared_ptr<AbstractPartition>, 
                                                         std::shared_ptr<AbstractIntermediateStore>)>;

  static MapToMapOutputManagerFuncT GetMapToMapOutputMangerFunc(PlanItem plan);
  static MapOutputToIntermediateStoreFuncT GetMapOutputToIntermediateStoreFunc(PlanItem plan);
  static MapToIntermediateStoreFuncT GetMapToIntermediateStoreFunc(PlanItem plan);
 private:
  std::map<int, MapToMapOutputManagerFuncT> map_to_map_output_storage_store_;
  std::map<int, MapToIntermediateStoreFuncT> map_to_intermediate_store_;
  std::map<int, MapOutputToIntermediateStoreFuncT> map_output_to_intermediate_store_;
};

}  // namespaca xyz

