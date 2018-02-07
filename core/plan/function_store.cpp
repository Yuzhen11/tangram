#include "core/plan/function_store.hpp"

#include "glog/logging.h"

namespace xyz {

const FunctionStore::PartToOutputManager& FunctionStore::GetMapPart1(int id) {
  CHECK(part_to_output_manager_.find(id) != part_to_output_manager_.end());
  return part_to_output_manager_[id];
}
const FunctionStore::MapOutputToIntermediate& FunctionStore::GetMapPart2(int id) {
  CHECK(mapoutput_to_intermediate_.find(id) != mapoutput_to_intermediate_.end());
  return mapoutput_to_intermediate_[id];
}
const FunctionStore::PartToIntermediate& FunctionStore::GetMap(int id) {
  CHECK(part_to_intermediate_.find(id) != part_to_intermediate_.end());
  return part_to_intermediate_[id];
}

FunctionStore::PartToOutputManager FunctionStore::GetPartToOutputManager(int id, PartToOutput map) {
  return [map, id](std::shared_ptr<AbstractPartition> partition, 
                std::shared_ptr<MapOutputManager> map_output_storage) {
    auto map_output = map(partition);
    map_output_storage->Add(id, map_output);
  };
}
FunctionStore::MapOutputToIntermediate FunctionStore::GetOutputsToIntermediate(int id, OutputsToBin merge_combine) {
  return [merge_combine, id](const std::vector<std::shared_ptr<AbstractMapOutput>>& map_outputs, 
                std::shared_ptr<AbstractIntermediateStore> intermediate_store,
                int part_id) {
    SArrayBinStream bin = merge_combine(map_outputs, part_id);
    // TODO Add msg header.
    Message msg = bin.ToMsg();
    intermediate_store->Add(msg);
  };
}
FunctionStore::PartToIntermediate FunctionStore::GetPartToIntermediate(PartToOutput map) {
  return [map](std::shared_ptr<AbstractPartition> partition, std::shared_ptr<AbstractIntermediateStore> intermediate_store) {
    // 1. map
    auto map_output = map(partition); 
    // 2. serialize
    auto bins = map_output->Serialize();
    // 3. add to intermediate_store
    for (auto& bin: bins) {
      Message msg = bin.ToMsg();
      // TODO Add msg header.
      intermediate_store->Add(msg);
    }
  };
}

void FunctionStore::AddPartToIntermediate(int id, PartToOutput func) {
  auto ret = GetPartToIntermediate(func);
  part_to_intermediate_.insert({id, ret});
}

void FunctionStore::AddPartToOutputManager(int id, PartToOutput func) {
  auto ret = GetPartToOutputManager(id, func);
  part_to_output_manager_.insert({id, ret});
}

void FunctionStore::AddOutputsToBin(int id, OutputsToBin func) {
  auto ret = GetOutputsToIntermediate(id, func);
  mapoutput_to_intermediate_.insert({id, ret});
}

}  // namespaca xyz

