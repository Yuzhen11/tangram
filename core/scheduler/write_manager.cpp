#include "core/scheduler/write_manager.hpp"


namespace xyz {

void WriteManager::Write(SpecWrapper s) {
  CHECK(s.type == SpecWrapper::Type::kWrite);
  auto* write_spec = static_cast<WriteSpec*>(s.spec.get());
  int id = write_spec->collection_id;
  int plan_id = s.id;
  cid_pid_[id] = plan_id;
  std::string url = write_spec->url;
  auto& collection_view = elem_->collection_map->Get(id);
  reply_count_map[id] = 0;
  expected_reply_count_map[id] = collection_view.mapper.GetNumParts();
  LOG(INFO) << "[Scheduler] writing to " << expected_reply_count_map[s.id] << " partitions";
  for (int i = 0; i < collection_view.mapper.GetNumParts(); ++ i) {
    int node_id = collection_view.mapper.Get(i);
    SArrayBinStream bin;
    std::string dest_url = url + "/part-" + std::to_string(i);
    bin << id << i << dest_url;  // collection_id, partition_id, url, plan_id
    SendTo(elem_, node_id, ScheduleFlag::kWritePartition, bin);
  }
}

void WriteManager::FinishWritePartition(SArrayBinStream bin) {
  int qid, collection_id;
  bin >> qid >> collection_id;
  reply_count_map[collection_id] += 1;
  if(reply_count_map[collection_id] == expected_reply_count_map[collection_id]){
    SArrayBinStream reply_bin;
    reply_bin << cid_pid_[collection_id];
    ToScheduler(elem_, ScheduleFlag::kFinishPlan, reply_bin);
  }
}

}
