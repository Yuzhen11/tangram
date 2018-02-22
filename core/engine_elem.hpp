#pragma once

#include <memory>

#include "base/node.hpp"
#include "core/executor/executor.hpp"
#include "core/partition/partition_manager.hpp"
#include "core/partition/partition_tracker.hpp"
#include "core/intermediate/simple_intermediate_store.hpp"
#include "core/plan/function_store.hpp"
#include "comm/abstract_sender.hpp"

namespace xyz {

struct EngineElem {
  Node node;
  std::shared_ptr<Executor> executor;
  std::shared_ptr<PartitionManager> partition_manager;
  std::shared_ptr<FunctionStore> function_store;
  std::shared_ptr<AbstractIntermediateStore> intermediate_store;
  std::shared_ptr<PartitionTracker> partition_tracker;
  std::shared_ptr<AbstractSender> sender;

  std::string namenode;
  int port;
};

}  // namespace xyz
