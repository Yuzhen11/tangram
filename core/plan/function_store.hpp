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
  using JoinFuncT = AbstractFunctionStore::JoinFuncT;
  using MapWith = AbstractFunctionStore::MapWith;

  // void AddPlanItem(PlanItem plan);
  // Partition -> MapOutputManager
  using PartToOutputManager = std::function<void(std::shared_ptr<AbstractPartition>, 
                                                        std::shared_ptr<MapOutputManager>)>;
  // MapOutput -> IntermediateStore
  using MapOutputToIntermediate = std::function<void(const std::vector<std::shared_ptr<AbstractMapOutput>>&, 
                                                               std::shared_ptr<AbstractIntermediateStore>,
                                                               int part_id)>;
  // Partition -> IntermediateStore
  using PartToIntermediate = std::function<void(std::shared_ptr<AbstractPartition>, 
                                                std::shared_ptr<AbstractIntermediateStore>)>;

  using PartWithToIntermediate = std::function<void(std::shared_ptr<AbstractPartition>,
                                                    std::shared_ptr<AbstractPartitionCache>,
                                                    std::shared_ptr<AbstractIntermediateStore>)>;

  // Used by engine.
  const PartToOutputManager& GetMapPart1(int id);
  const MapOutputToIntermediate& GetMapPart2(int id);
  const PartToIntermediate& GetMap(int id);
  const JoinFuncT& GetJoin(int id);

  // Used by plan to register function.
  virtual void AddPartToIntermediate(int id, PartToOutput func) override;
  virtual void AddPartToOutputManager(int id, PartToOutput func) override;
  virtual void AddOutputsToBin(int id, OutputsToBin func) override;
  virtual void AddJoinFunc(int id, JoinFuncT func) override;
  virtual void AddMapWith(int id, MapWith func) override;

  // For test use.
  static PartToOutputManager GetPartToOutputManager(int id, PartToOutput map);
  static MapOutputToIntermediate GetOutputsToIntermediate(int id, OutputsToBin merge_combine);
  static PartToIntermediate GetPartToIntermediate(PartToOutput map);

 private:
  std::map<int, PartToOutputManager> part_to_output_manager_;
  std::map<int, PartToIntermediate> part_to_intermediate_;
  std::map<int, MapOutputToIntermediate> mapoutput_to_intermediate_;
  std::map<int, JoinFuncT> join_functions;
  std::map<int, PartWithToIntermediate> partwith_to_intermediate_;
};

}  // namespaca xyz

