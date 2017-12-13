#include "core/engine.hpp"

#include <chrono>

namespace xyz {

Engine::Engine(int thread_pool_size, std::unique_ptr<AbstractPartitionManager>&& partition_manager, 
        std::unique_ptr<AbstractOutputManager>&& output_manager)
    : thread_pool_(thread_pool_size), thread_pool_size_(thread_pool_size),
      partition_manager_(std::move(partition_manager)), output_manager_(std::move(output_manager)){}

Engine::~Engine() {
  // TODO cautions: when the partition_manager destroyed, some partitions may still be
  // referenced by the item in the thread_pool_
  // wait until all items in the thread_pool_ finish
  while (thread_pool_.size()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

void Engine::RunPlanItem(int plan_id, int phase, std::shared_ptr<AbstractPartition> partition) {
  auto it = plans_.find(plan_id);
  CHECK(it != plans_.end()) << "plan_id not found: " << plan_id; 
  if (phase == 0) {
    const auto& map = it->second.GetMap();
    thread_pool_.enqueue(map, partition, output_manager_.get());
  } else if (phase == 1) {
    // TODO
  } else {
    CHECK(false) << "unknown phase: " << phase;
  }
}

void Engine::RunPlanItem(int plan_id, int phase, int collection_id, int partition_id) {
  auto part = partition_manager_->Get(collection_id, partition_id);
  RunPlanItem(plan_id, phase, part);
}

void Engine::AddPlan(int plan_id, PlanItem plan_item) {
  CHECK(plans_.find(plan_id) == plans_.end());
  plans_.insert(std::make_pair(plan_id, std::move(plan_item)));
}

void Engine::Main() {
  // TODO: If thread pool queue is empty, fetch task from scheduler.
}

}  // namespace

