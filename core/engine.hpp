#pragma once

#include <map>
#include <memory>

#include "core/executor/executor.hpp"
#include "core/partition/partition_manager.hpp"
#include "core/map_output/abstract_map_output.hpp"
#include "core/plan/function_store.hpp"
#include "core/intermediate/simple_intermediate_store.hpp"
#include "core/plan/plan_spec.hpp"
#include "core/progress_tracker.hpp"

namespace xyz {

class Engine {
 public:
  Engine(int thread_pool_size);
  ~Engine();

  void RunLocalPartitions(PlanSpec plan);

  void Main();
 private:
  int thread_pool_size_;
  std::unique_ptr<PartitionManager> partition_manager_;
  std::unique_ptr<Executor> executor_;
  std::unique_ptr<FunctionStore> function_store_;
  std::shared_ptr<AbstractIntermediateStore> intermediate_store_;
  std::unique_ptr<ProgressTracker> tracker_;
};

}  // namespace

