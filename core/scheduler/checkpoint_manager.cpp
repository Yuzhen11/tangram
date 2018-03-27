#include "core/scheduler/checkpoint_manager.hpp"


namespace xyz {

void CheckpointManager::Checkpoint(SpecWrapper s) {
  CHECK(s.type == SpecWrapper::Type::kCheckpoint);
  auto* checkpoint_spec = static_cast<CheckpointSpec*>(s.spec.get());
  int cid = checkpoint_spec->cid;
  std::string url = checkpoint_spec->url;
  auto& collection_view = elem_->collection_map->Get(cid);
  cid_pid_[cid] = s.id;
  checkpoint_reply_count_map[cid] = 0;
  expected_checkpoint_reply_count_map[cid] = collection_view.mapper.GetNumParts();
  for (int i = 0; i < collection_view.mapper.GetNumParts(); ++ i) {
    int node_id = collection_view.mapper.Get(i);
    SArrayBinStream bin;
    std::string dest_url = url + "/part-" + std::to_string(i);
    bin << cid << i << dest_url;  // collection_id, partition_id, url
    SendTo(elem_, node_id, ScheduleFlag::kCheckpoint, bin);
  }
  collection_status_->AddCP(cid, url);  // add checkpoint here
}

void CheckpointManager::LoadCheckpoint(SpecWrapper s) {
  CHECK(s.type == SpecWrapper::Type::kLoadCheckpoint);
  auto* load_checkpoint_spec = static_cast<LoadCheckpointSpec*>(s.spec.get());
  int cid = load_checkpoint_spec->cid;
  std::string url = load_checkpoint_spec->url;
  checkpoint_loader_->LoadCheckpoint(cid, url, [this, cid]() {
    SArrayBinStream reply_bin;
    reply_bin << cid_pid_[cid];
    ToScheduler(elem_, ScheduleFlag::kFinishPlan, reply_bin);
  });
}

void CheckpointManager::FinishCheckpoint(SArrayBinStream bin) {
  int qid, collection_id;
  bin >> qid >> collection_id;
  checkpoint_reply_count_map[collection_id] += 1;
  if (checkpoint_reply_count_map[collection_id] == expected_checkpoint_reply_count_map[collection_id]){
    SArrayBinStream reply_bin;
    reply_bin << cid_pid_[collection_id];
    ToScheduler(elem_, ScheduleFlag::kFinishPlan, reply_bin);
  }
}

}
