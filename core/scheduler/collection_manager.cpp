#include "core/scheduler/collection_manager.hpp"

namespace xyz {

void CollectionManager::Update(SArrayBinStream bin) {
  std::pair<int,int> pid_cid;  // plan_id, collection_id
  bin >> pid_cid;
  LOG(INFO) << "[Scheduler] UpdateCollection for {plan_id, collection_id}: {" 
      << pid_cid.first << "," << pid_cid.second << "}";
  auto& collection_view = elem_->collection_map->Get(pid_cid.second);

  received_replies_[pid_cid].clear();
  SArrayBinStream reply_bin;
  reply_bin << pid_cid << collection_view;
  SendToAllWorkers(elem_, ScheduleFlag::kUpdateCollection, reply_bin);
}

void CollectionManager::FinishUpdate(SArrayBinStream bin) {
  std::pair<int,int> pid_cid;  // plan_id, collection_id
  int node_id;
  bin >> pid_cid >> node_id;
  received_replies_[pid_cid].insert(node_id);
  if (received_replies_[pid_cid].size() == elem_->nodes.size()) {
    received_replies_[pid_cid].clear();
    LOG(INFO) << "[Scheduler] UpdateCollection for {plan_id, collection_id}: {" 
        << pid_cid.first << "," << pid_cid.second << "} done";
    // finish_plan
    SArrayBinStream reply_bin;
    reply_bin << pid_cid.first;
    ToScheduler(elem_, ScheduleFlag::kFinishPlan, reply_bin);
  }
}

}  // namespace xyz

