#pragma once

#include <functional>
#include <memory>

#include "core/intermediate/abstract_intermediate_store.hpp"
#include "core/map_output/abstract_map_output.hpp"
#include "core/partition/abstract_partition.hpp"
#include "core/cache/abstract_partition_cache.hpp"
#include "core/cache/fetcher.hpp"
#include "core/partition/abstract_map_progress_tracker.hpp"
#include "io/abstract_block_reader.hpp"
#include "io/abstract_writer.hpp"

namespace xyz { 

class AbstractFunctionStore {
 public:
  using MapFuncT = std::function<std::shared_ptr<AbstractMapOutput>(
          std::shared_ptr<AbstractPartition>, 
          std::shared_ptr<AbstractMapProgressTracker>)>;
  using MergeCombineFuncT = std::function<SArrayBinStream(const std::vector<std::shared_ptr<AbstractMapOutput>>& map_outputs, int part_id)>;
  using JoinFuncT = std::function<void (std::shared_ptr<AbstractPartition>, SArrayBinStream)>;
  using MapWith = 
      std::function<std::shared_ptr<AbstractMapOutput>(std::shared_ptr<AbstractPartition>,
                                                       std::shared_ptr<Fetcher>,
                                                       std::shared_ptr<AbstractMapProgressTracker>)>;
  using CreatePartFromBinFuncT = std::function<std::shared_ptr<AbstractPartition>(
          SArrayBinStream bin, int part_id, int num_part)>;
  using CreatePartFromBlockReaderFuncT = std::function<std::shared_ptr<AbstractPartition>(
          std::shared_ptr<AbstractBlockReader>)>;
  using WritePartFuncT = std::function<void(std::shared_ptr<AbstractPartition>, std::shared_ptr<AbstractWriter>, std::string)>;
  using GetterFuncT = std::function<
      SArrayBinStream(SArrayBinStream& bin, std::shared_ptr<AbstractPartition>)>;

  ~AbstractFunctionStore(){}
  virtual void AddMap(int id, MapFuncT func) = 0;
  virtual void AddMergeCombine(int id, MergeCombineFuncT func) = 0;
  virtual void AddJoin(int id, JoinFuncT func) = 0;
  virtual void AddMapWith(int id, MapWith func) = 0;

  virtual void AddCreatePartFromBinFunc(int id, CreatePartFromBinFuncT func) = 0;
  virtual void AddCreatePartFromBlockReaderFunc(int id, CreatePartFromBlockReaderFuncT func) = 0;
  virtual void AddWritePart(int id, WritePartFuncT func) = 0;
  virtual void AddGetter(int id, GetterFuncT func) = 0;
};

}  // namespaca xyz

