#pragma once

#include <functional>
#include <memory>

#include "core/intermediate/abstract_intermediate_store.hpp"
#include "core/map_output/abstract_map_output.hpp"
#include "core/partition/abstract_partition.hpp"
#include "core/cache/abstract_partition_cache.hpp"

namespace xyz { 

class AbstractFunctionStore {
 public:
  using PartToOutput = std::function<std::shared_ptr<AbstractMapOutput>(std::shared_ptr<AbstractPartition>)>;
  using OutputsToBin = std::function<SArrayBinStream(const std::vector<std::shared_ptr<AbstractMapOutput>>& map_outputs, int part_id)>;
  using MapWith = 
      std::function<std::shared_ptr<AbstractMapOutput>(std::shared_ptr<AbstractPartition>,
                                                       std::shared_ptr<AbstractPartitionCache>)>;
  ~AbstractFunctionStore(){}
  virtual void AddPartToIntermediate(int id, PartToOutput func) = 0;
  virtual void AddPartToOutputManager(int id, PartToOutput func) = 0;
  virtual void AddOutputsToBin(int id, OutputsToBin func) = 0;
  virtual void AddMapWith(int id, MapWith func) = 0;
};

}  // namespaca xyz

