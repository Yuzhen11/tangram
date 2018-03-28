#include "core/scheduler/recover_manager.hpp"

namespace xyz {
void RecoverManager::Recover(int plan_id, std::set<int> mutable_collection,
                std::set<int> immutable_collection,
                std::set<int> dead_nodes) {
  for (auto cid: mutable_collection) {
    ReplaceDeadnodesAndReturnUpdated(cid, dead_nodes);
    // TODO: load checkpoint for all partition
    // the logic may be similar to checkpoint manager, we can just 
    // copy the code for now. 
    std::string url = collection_status_->GetLastCP(cid);
    checkpoint_loader_->LoadCheckpoint(cid, url, [this, plan_id]() {
      SArrayBinStream reply_bin;
      reply_bin << plan_id;
      ToScheduler(elem_, ScheduleFlag::kFinishPlan, reply_bin);
    });
  }
  for (auto cid: immutable_collection) {
    auto updates = ReplaceDeadnodesAndReturnUpdated(cid, dead_nodes);
    // TODO: load checkpoint for the updates
  }

}

std::vector<std::pair<int, int>> RecoverManager::ReplaceDeadnodesAndReturnUpdated(int cid, std::set<int> dead_nodes) {
  std::vector<std::pair<int,int>> updates;
  auto& collection_view = elem_->collection_map->Get(cid);
  auto& part_to_node = collection_view.mapper.Mutable();
  auto live_node_iter = elem_->nodes.begin();
  for (int i = 0; i < part_to_node.size(); ++ i) {
    int node_id = part_to_node[i];
    // if a node is a dead node, replace it with a live node
    if (dead_nodes.find(node_id) != dead_nodes.end()) {
      part_to_node[i] = live_node_iter->second.node.id;
      updates.push_back({i, part_to_node[i]});
      live_node_iter++;
      if (live_node_iter == elem_->nodes.end()) {
        // round-robin assign the live node to it.
        live_node_iter = elem_->nodes.begin();
      }
    }
  }
  return updates;
}

}  // namespace xyz
