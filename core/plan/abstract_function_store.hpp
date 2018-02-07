#pragma once

#include <functional>
#include <memory>

#include "core/intermediate/abstract_intermediate_store.hpp"
#include "core/map_output/abstract_map_output.hpp"
#include "core/partition/abstract_partition.hpp"

namespace xyz { 

class AbstractFunctionStore {
 public:
  using PartToOutput = std::function<std::shared_ptr<AbstractMapOutput>(std::shared_ptr<AbstractPartition>)>;
  using OutputsToBin = std::function<SArrayBinStream(const std::vector<std::shared_ptr<AbstractMapOutput>>& map_outputs, int part_id)>;
  using JoinFuncT = std::function<void (std::shared_ptr<AbstractPartition>, SArrayBinStream)>;
  ~AbstractFunctionStore(){}
  virtual void AddPartToIntermediate(int id, PartToOutput func) = 0;
  virtual void AddPartToOutputManager(int id, PartToOutput func) = 0;
  virtual void AddOutputsToBin(int id, OutputsToBin func) = 0;
  virtual void AddJoinFunc(int id, JoinFuncT func) = 0;
};

}  // namespaca xyz

