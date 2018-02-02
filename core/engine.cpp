#include "core/engine.hpp"

#include <chrono>

namespace xyz {

Engine::Engine(int thread_pool_size)
    : executor_(new Executor(thread_pool_size)), thread_pool_size_(thread_pool_size),
      partition_manager_(new PartitionManager),
      function_store_(new FunctionStore),
      intermediate_store_(new SimpleIntermediateStore){}

Engine::~Engine() {
}

void Engine::AddPlan(PlanItem plan_item) {
  int plan_id = plan_item.plan_id;
  CHECK(plans_.find(plan_id) == plans_.end());
  plans_.insert(std::make_pair(plan_id, std::move(plan_item)));
  function_store_->AddPlanItem(plan_item);
}

void Engine::RunLocalPartitions(int plan_id) {
  CHECK(plans_.find(plan_id) != plans_.end()) << "plan does not exist, id: " << plan_id;
  int collection_id = plans_.find(plan_id)->second.map_collection_id;
  auto& func = function_store_->GetMapToIntermediateStoreFunc(plan_id);
  auto& parts = partition_manager_->Get(collection_id);
  for (auto& part : parts) {
    executor_->Add([this, part, func](){ func(part.second->partition, intermediate_store_); });
  }
}

void Engine::Main() {
  // TODO: If thread pool queue is empty, fetch task from scheduler.
}

}  // namespace

