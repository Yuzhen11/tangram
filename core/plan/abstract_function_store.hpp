#pragma once

#include <functional>
#include <memory>

#include "core/intermediate/abstract_intermediate_store.hpp"
#include "core/map_output/abstract_map_output.hpp"
#include "core/partition/abstract_partition.hpp"
#include "core/cache/abstract_partition_cache.hpp"
#include "core/partition/abstract_map_progress_tracker.hpp"
#include "io/abstract_block_reader.hpp"

namespace xyz { 

class AbstractFunctionStore {
 public:
  using PartToOutput = std::function<std::shared_ptr<AbstractMapOutput>(
          std::shared_ptr<AbstractPartition>, 
          std::shared_ptr<AbstractMapProgressTracker>)>;
  using OutputsToBin = std::function<SArrayBinStream(const std::vector<std::shared_ptr<AbstractMapOutput>>& map_outputs, int part_id)>;
  using JoinFuncT = std::function<void (std::shared_ptr<AbstractPartition>, SArrayBinStream)>;
  using MapWith = 
      std::function<std::shared_ptr<AbstractMapOutput>(std::shared_ptr<AbstractPartition>,
                                                       std::shared_ptr<AbstractPartitionCache>,
                                                       std::shared_ptr<AbstractMapProgressTracker>)>;
  using CreatePartFromBinFuncT = std::function<std::shared_ptr<AbstractPartition>(
          SArrayBinStream bin, int part_id, int num_part)>;
  using CreatePartFromBlockReaderFuncT = std::function<std::shared_ptr<AbstractPartition>(
          std::shared_ptr<AbstractBlockReader>)>;

  ~AbstractFunctionStore(){}
  virtual void AddPartToIntermediate(int id, PartToOutput func) = 0;
  virtual void AddPartToOutputManager(int id, PartToOutput func) = 0;
  virtual void AddOutputsToBin(int id, OutputsToBin func) = 0;
  virtual void AddJoinFunc(int id, JoinFuncT func) = 0;
  virtual void AddMapWith(int id, MapWith func) = 0;

  virtual void AddCreatePartFromBinFunc(int id, CreatePartFromBinFuncT func) = 0;
  virtual void AddCreatePartFromBlockReaderFunc(int id, CreatePartFromBlockReaderFuncT func) = 0;
};

}  // namespaca xyz

