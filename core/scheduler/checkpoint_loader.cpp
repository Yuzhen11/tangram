#include "core/scheduler/checkpoint_loader.hpp"

namespace xyz {

void CheckpointLoader::LoadCheckpoint(int cid, std::string url,
        std::function<void()> f) {
  LOG(INFO) << "[CheckpointLoader] loading checkpoint for collection: " << cid;
  auto& collection_view = elem_->collection_map->Get(cid);
  loadcheckpoint_reply_count_map_[cid] = 0;
  expected_loadcheckpoint_reply_count_map_[cid] = collection_view.mapper.GetNumParts();
  callbacks_.insert({cid, f});

  for (int i = 0; i < collection_view.mapper.GetNumParts(); ++ i) {
    int node_id = collection_view.mapper.Get(i);
    SendLoadCommand(cid, i, node_id, url);
  }
}

void CheckpointLoader::LoadCheckpointPartial(int cid, std::string url,
        std::vector<int> parts,
        std::function<void()> f) {
  LOG(INFO) << "[CheckpointLoader] loading checkpoint (partial) for collection: " << cid << ", parts size: " << parts.size();
  loadcheckpoint_reply_count_map_[cid] = 0;
  callbacks_.insert({cid, f});
  expected_loadcheckpoint_reply_count_map_[cid] = parts.size();

  auto& collection_view = elem_->collection_map->Get(cid);
  auto& part_to_node_map = collection_view.mapper.Get();
  for (auto part_id: parts) {
    CHECK_LT(part_id, part_to_node_map.size());
    int node_id = part_to_node_map[part_id];
    SendLoadCommand(cid, part_id, node_id, url);
  }
}

void CheckpointLoader::SendLoadCommand(int cid, int part_id, int node_id, std::string url) {
  SArrayBinStream bin;
  std::string dest_url = url + "/c" + std::to_string(cid) + "-p" + std::to_string(part_id);
  bin << cid << part_id << dest_url;  // collection_id, partition_id, url
  SendTo(elem_, node_id, ScheduleFlag::kLoadCheckpoint, bin);
}

void CheckpointLoader::FinishLoadCheckpoint(SArrayBinStream bin) {
  int qid, collection_id;
  bin >> qid >> collection_id;
  loadcheckpoint_reply_count_map_[collection_id] += 1;
  if (loadcheckpoint_reply_count_map_[collection_id] == expected_loadcheckpoint_reply_count_map_[collection_id]){
    CHECK(callbacks_.find(collection_id) != callbacks_.end());
    callbacks_[collection_id]();  // invoke the callback
    callbacks_.erase(collection_id);
  }
}

}  // namespace xyz

