#pragma once

#include <memory>

#include "base/node.hpp"
#include "core/executor/executor.hpp"
#include "core/partition/partition_manager.hpp"
#include "core/intermediate/simple_intermediate_store.hpp"
#include "core/plan/function_store.hpp"
#include "comm/abstract_sender.hpp"
#include "core/collection_map.hpp"
#include "core/cache/fetcher.hpp"

namespace xyz {

struct EngineElem {
  Node node;
  std::shared_ptr<Executor> executor;
  std::shared_ptr<PartitionManager> partition_manager;
  std::shared_ptr<FunctionStore> function_store;
  std::shared_ptr<AbstractIntermediateStore> intermediate_store;
  std::shared_ptr<AbstractSender> sender;
  std::shared_ptr<CollectionMap> collection_map;
  std::shared_ptr<Fetcher> fetcher;

  std::string namenode;
  int port;

  int num_local_threads;
  int num_join_threads;
  int num_combine_threads;
};

}  // namespace xyz

