#pragma once

#include <map>
#include <memory>

#include "base/node.hpp"
#include "comm/abstract_sender.hpp"
#include "core/collection_map.hpp"

namespace xyz {

struct NodeInfo {
  Node node;
  int num_local_threads;
};

struct SchedulerElem {
  std::shared_ptr<AbstractSender> sender;
  std::shared_ptr<CollectionMap> collection_map;
  std::map<int, NodeInfo> nodes;
};

} // namespace xyz

