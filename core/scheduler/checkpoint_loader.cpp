#include "core/scheduler/checkpoint_loader.hpp"

namespace xyz {

void CheckpointLoader::LoadCheckpoint(int cid, std::string url,
        std::function<void()> f) {
  auto& collection_view = elem_->collection_map->Get(cid);
  loadcheckpoint_reply_count_map_[cid] = 0;
  expected_loadcheckpoint_reply_count_map_[cid] = collection_view.mapper.GetNumParts();
  callbacks_.insert({cid, f});

  for (int i = 0; i < collection_view.mapper.GetNumParts(); ++ i) {
    int node_id = collection_view.mapper.Get(i);
    SArrayBinStream bin;
    std::string dest_url = url + "/part-" + std::to_string(i);
    bin << cid << i << dest_url;  // collection_id, partition_id, url
    SendTo(elem_, node_id, ScheduleFlag::kLoadCheckpoint, bin);
  }
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

