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

void Engine::RunLocalPartitions(PlanSpec plan) {
  auto& func = function_store_->GetMap(plan.plan_id);
  auto& parts = partition_manager_->Get(plan.map_collection_id);
  for (auto part : parts) {
    executor_->Add([this, part, func](){ func(part.second->partition, intermediate_store_); });
  }
}

void Engine::Main() {
  // TODO: If thread pool queue is empty, fetch task from scheduler.
}

}  // namespace

