#pragma once

#include <map>
#include <memory>

#include "base/node.hpp"
#include "base/sarray_binstream.hpp"
#include "comm/abstract_sender.hpp"
#include "core/collection_map.hpp"
#include "core/scheduler/control.hpp"
#include "core/queue_node_map.hpp"

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

void SendToAllWorkers(std::shared_ptr<SchedulerElem> elem, ScheduleFlag flag, SArrayBinStream bin);
void SendTo(std::shared_ptr<SchedulerElem> elem, int node_id, ScheduleFlag flag, SArrayBinStream bin);
void ToScheduler(std::shared_ptr<SchedulerElem> elem, ScheduleFlag flag, SArrayBinStream bin);


} // namespace xyz

