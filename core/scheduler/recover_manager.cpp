#include "core/scheduler/recover_manager.hpp"

#include <sstream>
#include "base/color.hpp"

namespace xyz {
void RecoverManager::Recover(std::set<int> dead_nodes) {
  CHECK_EQ(started_, false);
  started_ = true;
  start_time_ = std::chrono::system_clock::now();

  recovering_collections_.clear();
  updating_collections_.clear();

  // recover the writes
  auto writes = collection_status_->GetWrites();
  for (auto w : writes) {
    recovering_collections_.insert(w);
    ReplaceDeadnodesAndReturnUpdated(w, dead_nodes);
    std::string url = collection_status_->GetLastCP(w);
    std::stringstream ss;
    ss << "Recovering write checkpoint (full) for plan: " << w << ", from: " << url;
    LOG(INFO) << RED(ss.str());
    checkpoint_loader_->LoadCheckpoint(w, url, [this, w]() {
      RecoverDoneForACollection(w, Type::LoadCheckpoint);
    });
  }

  // recover the reads
  auto reads = collection_status_->GetReads();
  for (auto r : reads) {
    recovering_collections_.insert(r);
    auto updates = ReplaceDeadnodesAndReturnUpdated(r, dead_nodes);
    std::string url = collection_status_->GetLastCP(r);

    std::stringstream ss;
    ss << "Recovering read checkpoint (partial) for plan: " << r << ", from: " << url;
    ss << ", recovering partial partitions: ";
    for (auto update: updates) {
      ss << update << " ";
    }
    LOG(INFO) << RED(ss.str());
    // only load the lost pasts.
    checkpoint_loader_->LoadCheckpointPartial(r, url, updates, [this, r]() {
      RecoverDoneForACollection(r, Type::LoadCheckpoint);
    });
  }
  
  // update collection map
  for (auto w : writes) {
    updating_collections_.insert(w);
    collection_manager_->Update(w, [this, w]() {
      RecoverDoneForACollection(w, Type::UpdateCollectionMap);
    });
  }
  for (auto r : reads) {
    updating_collections_.insert(r);
    collection_manager_->Update(r, [this, r]() {
      RecoverDoneForACollection(r, Type::UpdateCollectionMap);
    });
  }
}

void RecoverManager::RecoverDoneForACollection(int cid, RecoverManager::Type type) {
  CHECK(started_);
  if (type == RecoverManager::Type::LoadCheckpoint) {
    CHECK(recovering_collections_.find(cid) != recovering_collections_.end());
    LOG(INFO) << "[RecoverManager] collection " << cid << " recovered from checkpoint";
    recovering_collections_.erase(cid);
  } else if (type == RecoverManager::Type::UpdateCollectionMap) {
    CHECK(updating_collections_.find(cid) != updating_collections_.end());
    LOG(INFO) << "[RecoverManager] collection " << cid << " collection_map updated";
    updating_collections_.erase(cid);
  } else {
    CHECK(false);
  }
  if (recovering_collections_.empty() && updating_collections_.empty()) {
    // all collections recovered, notify scheduler
    auto current_time = std::chrono::system_clock::now();
    std::chrono::duration<double> duration = current_time - start_time_;
    LOG(INFO) << RED("All collections recovered! Recovery time: " + std::to_string(duration.count()) + " ms");
    started_ = false;
    SArrayBinStream reply_bin;
    ToScheduler(elem_, ScheduleFlag::kFinishRecovery, reply_bin);
  }
}

std::vector<int> RecoverManager::ReplaceDeadnodesAndReturnUpdated(int cid, std::set<int> dead_nodes) {
  std::vector<int> updates;
  auto& collection_view = elem_->collection_map->Get(cid);
  auto& part_to_node = collection_view.mapper.Mutable();
  auto live_node_iter = elem_->nodes.begin();
  for (int i = 0; i < part_to_node.size(); ++ i) {
    int node_id = part_to_node[i];
    // if a node is a dead node, replace it with a live node
    if (dead_nodes.find(node_id) != dead_nodes.end()) {
      part_to_node[i] = live_node_iter->second.node.id;
      updates.push_back(i);
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
