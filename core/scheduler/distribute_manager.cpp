#include "core/scheduler/distribute_manager.hpp"

namespace xyz {

void DistributeManager::Distribute(SpecWrapper spec_wrapper) {
  auto spec = static_cast<DistributeSpec*>(spec_wrapper.spec.get());
  LOG(INFO) << "[Scheduler] Distribute {plan_id, collection_id}: {" 
      << spec_wrapper.id << "," << spec->collection_id << "}";
  part_expected_map_[spec_wrapper.id] = spec->num_partition;
  // round-robin
  auto node_iter = elem_->nodes.begin();
  for (int i = 0; i < spec->num_partition; ++i) {
    CHECK(node_iter != elem_->nodes.end());
    Message msg;
    msg.meta.sender = 0;
    msg.meta.recver = GetWorkerQid(node_iter->second.node.id);
    msg.meta.flag = Flag::kOthers;
    SArrayBinStream ctrl_bin, bin;
    ctrl_bin << ScheduleFlag::kDistribute;
    bin << i << spec_wrapper.id;
    spec->ToBin(bin);
    msg.AddData(ctrl_bin.ToSArray());
    msg.AddData(bin.ToSArray());
    elem_->sender->Send(std::move(msg));

    node_iter++;
    if (node_iter == elem_->nodes.end()) {
      node_iter = elem_->nodes.begin();
    }
  }
}

void DistributeManager::FinishDistribute(SArrayBinStream bin) {
  int collection_id, part_id, node_id, plan_id;
  bin >> collection_id >> part_id >> node_id >> plan_id;
  distribute_map_[collection_id][part_id] = node_id;
  if (distribute_map_[collection_id].size() == part_expected_map_[plan_id]) {
    LOG(INFO) << "[Scheduler] Distribute {plan_id, collection_id}: {" 
        << plan_id << "," << collection_id << "} done";
    // construct the collection view
    std::vector<int> part_to_node(part_expected_map_[plan_id]);
    for (int i = 0; i < part_to_node.size(); ++i) {
      CHECK(distribute_map_[collection_id].find(i) !=
            distribute_map_[collection_id].end());
      part_to_node[i] = distribute_map_[collection_id][i];
    }
    CollectionView cv;
    cv.collection_id = collection_id;
    cv.mapper = SimplePartToNodeMapper(part_to_node);
    cv.num_partition = cv.mapper.GetNumParts();
    elem_->collection_map->Insert(cv);

    // trigger InitWorkers
    SArrayBinStream reply_bin;
    std::pair<int,int> pid_cid{plan_id, collection_id};
    reply_bin << pid_cid;
    ToScheduler(elem_, ScheduleFlag::kUpdateCollection, reply_bin);
  }
}

}  // namespace xyz
