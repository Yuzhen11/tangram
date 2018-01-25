#include "core/plan/function_store.hpp"

#include "glog/logging.h"

namespace xyz {

FunctionStore::MapToMapOutputManagerFuncT FunctionStore::GetMapToMapOutputMangerFunc(PlanItem plan) {
  // Wrap the plan.map into a function which runs the map function and then put it into
  // map_output_storage.
  return [plan](std::shared_ptr<AbstractPartition> partition, 
                std::shared_ptr<MapOutputManager> map_output_storage) {
    CHECK(plan.map != nullptr);
    auto map_output = plan.map(partition);
    map_output_storage->Add(plan.plan_id, map_output);
  };
}
FunctionStore::MapOutputToIntermediateStoreFuncT FunctionStore::GetMapOutputToIntermediateStoreFunc(PlanItem plan) {
  // Wrap the plan.merge_combine into a function
  // which runs the merge_combine function and then put the result into intermediate_store
  return [plan](const std::vector<std::shared_ptr<AbstractMapOutput>>& map_outputs, 
                std::shared_ptr<AbstractIntermediateStore> intermediate_store,
                int part_id) {
    CHECK(plan.merge_combine != nullptr);
    SArrayBinStream bin = plan.merge_combine(map_outputs, part_id);
    // TODO Add msg header.
    Message msg = bin.ToMsg();
    intermediate_store->Add(msg);
  };
}
FunctionStore::MapToIntermediateStoreFuncT FunctionStore::GetMapToIntermediateStoreFunc(PlanItem plan) {
  return [plan](std::shared_ptr<AbstractPartition> partition, std::shared_ptr<AbstractIntermediateStore> intermediate_store) {
    // 1. map
    CHECK(plan.map != nullptr);
    auto map_output = plan.map(partition); 
    // 2. combine if needed
    if (plan.combine_type == PlanItem::CombineType::Combine) {  // need to combine
      CHECK(plan.combine != nullptr);
      plan.combine(map_output);
    }
    // 3. serialize
    auto bins = map_output->Serialize();
    // 4. add to intermediate_store
    for (auto& bin: bins) {
      Message msg = bin.ToMsg();
      // TODO Add msg header.
      intermediate_store->Add(msg);
    }
  };
}

void FunctionStore::AddPlanItem(PlanItem plan) {
  if (plan.combine_type == PlanItem::CombineType::MergeCombine) {  
    // merge_combine is enabled.
    map_to_map_output_storage_store_.insert({
        plan.plan_id, GetMapToMapOutputMangerFunc(plan)
    });
    map_output_to_intermediate_store_.insert({
        plan.plan_id, GetMapOutputToIntermediateStoreFunc(plan)
    });
  }
  if (plan.combine_type == PlanItem::CombineType::Combine
        || plan.combine_type == PlanItem::CombineType::None) {  
    map_to_intermediate_store_.insert({
        plan.plan_id, GetMapToIntermediateStoreFunc(plan)
    });
  }
}

}  // namespaca xyz

