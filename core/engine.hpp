#pragma once

#include <map>
#include <memory>

#include "core/executor/executor.hpp"
#include "core/partition/partition_manager.hpp"
#include "core/map_output/abstract_map_output.hpp"
#include "core/plan/plan_item.hpp"
#include "core/plan/function_store.hpp"
#include "core/intermediate/simple_intermediate_store.hpp"

namespace xyz {

class Engine {
 public:
  Engine(int thread_pool_size);
  ~Engine();
  void RunPlanItem(int plan_id, int phase, std::shared_ptr<AbstractPartition> partition);
  /*
   * Run by the contorller, push the lambda of the planitem into the threadpool.
   * Called during the runtime.
   */
  void RunPlanItem(int plan_id, int phase, int collection_id, int partition_id);
  /*
   * Add a plan to the engine.
   * Called in the plan construction phase.
   */
  void AddPlan(PlanItem plan_item);

  void RunLocalPartitions(int plan_id);
  void Main();
 private:
  int thread_pool_size_;
  std::map<int, PlanItem> plans_;
  std::unique_ptr<PartitionManager> partition_manager_;
  std::unique_ptr<Executor> executor_;
  std::unique_ptr<FunctionStore> function_store_;
  std::shared_ptr<AbstractIntermediateStore> intermediate_store_;
};

}  // namespace

