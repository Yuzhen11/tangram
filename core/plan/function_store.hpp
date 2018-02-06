#pragma once

#include <map>


#include "core/plan/abstract_function_store.hpp"

#include "core/map_output/map_output_storage.hpp"

namespace xyz { 

/*
 * Store all the functions.
 */
class FunctionStore : public AbstractFunctionStore {
 public:
  using PartToOutput = AbstractFunctionStore::PartToOutput;
  using OutputsToBin = AbstractFunctionStore::OutputsToBin;
  // void AddPlanItem(PlanItem plan);
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

  const MapToMapOutputManagerFuncT& GetMapToMapOutputMangerFunc(int id);
  const MapOutputToIntermediateStoreFuncT& GetMapOutputToIntermediateStoreFunc(int id);
  const MapToIntermediateStoreFuncT& GetMapToIntermediateStoreFunc(int id);

  virtual void AddPartToIntermediate(int id, PartToOutput func) override;
  virtual void AddPartToOutputManager(int id, PartToOutput func) override;
  virtual void AddOutputsToBin(int id, OutputsToBin func) override;

  // For test use.
  static MapToMapOutputManagerFuncT GetPartToOutputManager(int id, PartToOutput map);
  static MapOutputToIntermediateStoreFuncT GetOutputsToIntermediate(int id, OutputsToBin merge_combine);
  static MapToIntermediateStoreFuncT GetPartToIntermediate(PartToOutput map);
 private:
  std::map<int, MapToMapOutputManagerFuncT> part_to_output_manager_;
  std::map<int, MapToIntermediateStoreFuncT> part_to_intermediate_;
  std::map<int, MapOutputToIntermediateStoreFuncT> mapoutput_to_intermediate_;
};

}  // namespaca xyz

