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
const FunctionStore::JoinFuncT& FunctionStore::GetJoin(int id) {
  CHECK(join_functions.find(id) != join_functions.end());
  return join_functions[id];
}

void FunctionStore::AddPartToIntermediate(int id, PartToOutput map) {
  auto ret = [this, map](ShuffleMeta meta, std::shared_ptr<AbstractPartition> partition, 
               std::shared_ptr<AbstractIntermediateStore> intermediate_store,
               std::shared_ptr<AbstractMapProgressTracker> tracker) {
    // 1. map
    auto map_output = map(partition, tracker); 
    // 2. serialize
    auto bins = map_output->Serialize();
    // 3. add to intermediate_store
    for (int i = 0; i < bins.size(); ++ i) {
      Message msg;
      msg.meta.sender = 0;
      CHECK(collection_map_);
      msg.meta.recver = collection_map_->Lookup(meta.collection_id, i);
      msg.meta.flag = Flag::kOthers;
      SArrayBinStream ctrl_bin;
      meta.part_id = i;  // set the part_id here
      ctrl_bin << meta;
      msg.AddData(ctrl_bin.ToSArray());
      msg.AddData(bins[i].ToSArray());
      intermediate_store->Add(msg);
    }
  };
  part_to_intermediate_.insert({id, ret});
}

void FunctionStore::AddPartToOutputManager(int id, PartToOutput map) {
  auto ret = [map, id](std::shared_ptr<AbstractPartition> partition, 
                   std::shared_ptr<MapOutputManager> map_output_storage,
                   std::shared_ptr<AbstractMapProgressTracker> tracker) {
    auto map_output = map(partition, tracker);
    map_output_storage->Add(id, map_output);
  };
  part_to_output_manager_.insert({id, ret});
}

void FunctionStore::AddOutputsToBin(int id, OutputsToBin merge_combine) {
  auto ret = [merge_combine, id](const std::vector<std::shared_ptr<AbstractMapOutput>>& map_outputs, 
                std::shared_ptr<AbstractIntermediateStore> intermediate_store,
                int part_id) {
    SArrayBinStream bin = merge_combine(map_outputs, part_id);
    // TODO Add msg header.
    Message msg = bin.ToMsg();
    intermediate_store->Add(msg);
  };
  mapoutput_to_intermediate_.insert({id, ret});
}

void FunctionStore::AddJoinFunc(int id, JoinFuncT func) {
  join_functions.insert({id, func});
}

void FunctionStore::AddMapWith(int id, MapWith func) {
  partwith_to_intermediate_.insert({id, [func](std::shared_ptr<AbstractPartition> partition,
                                           std::shared_ptr<AbstractPartitionCache> partition_cache,
                                           std::shared_ptr<AbstractIntermediateStore> intermediate_store,
                                           std::shared_ptr<AbstractMapProgressTracker> tracker) {
    // 1. map
    auto map_output = func(partition, partition_cache, tracker); 
    // 2. serialize
    auto bins = map_output->Serialize();
    // 3. add to intermediate_store
    for (auto& bin: bins) {
      Message msg = bin.ToMsg();
      // TODO Add msg header.
      intermediate_store->Add(msg);
    }
  }});
}

}  // namespaca xyz

