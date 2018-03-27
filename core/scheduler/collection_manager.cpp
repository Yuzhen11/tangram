#include "core/scheduler/collection_manager.hpp"

namespace xyz {

void CollectionManager::Update(int collection_id, std::function<void()> f) {
  LOG(INFO) << "[Scheduler] UpdateCollection for collection_id: " 
      << collection_id;
  callbacks_.insert({collection_id, f});
  received_replies_[collection_id].clear();

  SArrayBinStream reply_bin;
  auto& collection_view = elem_->collection_map->Get(collection_id);
  reply_bin << collection_id << collection_view;
  SendToAllWorkers(elem_, ScheduleFlag::kUpdateCollection, reply_bin);
}

void CollectionManager::FinishUpdate(SArrayBinStream bin) {
  int collection_id;
  int node_id;
  bin >> collection_id >> node_id;
  received_replies_[collection_id].insert(node_id);
  if (received_replies_[collection_id].size() == elem_->nodes.size()) {
    received_replies_[collection_id].clear();
    LOG(INFO) << "[Scheduler] UpdateCollection for collection_id: "
        << collection_id << " done";
    // finish_plan
    // invoke the callback
    CHECK(callbacks_.find(collection_id) != callbacks_.end());
    callbacks_[collection_id]();  // invoke the callback
    callbacks_.erase(collection_id);
  }
}

}  // namespace xyz

