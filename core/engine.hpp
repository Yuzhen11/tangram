#pragma once

#include <map>
#include <memory>

#include "core/thread_pool.hpp"
#include "core/partition/abstract_partition_manager.hpp"
#include "core/map_output/abstract_map_output.hpp"
#include "core/plan_item.hpp"

namespace xyz {

class Engine {
 public:
  Engine(int thread_pool_size, std::unique_ptr<AbstractPartitionManager>&&, std::shared_ptr<AbstractMapOutput>&&);
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
  void AddPlan(int plan_id, PlanItem plan_item);
  void Main();
 private:
  int thread_pool_size_;
  ThreadPool thread_pool_;
  std::map<int, PlanItem> plans_;
  std::unique_ptr<AbstractPartitionManager> partition_manager_;
  std::shared_ptr<AbstractMapOutput> map_output_;
};

}  // namespace

