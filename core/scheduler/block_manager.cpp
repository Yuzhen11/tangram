#include "core/scheduler/block_manager.hpp"

namespace xyz {

BlockManager::BlockManager(std::shared_ptr<SchedulerElem> elem,
        std::function<std::shared_ptr<Assigner>()> builder)
    : elem_(elem), builder_(builder) {}

void BlockManager::Load(LoadSpec* spec) {
  std::vector<std::pair<std::string, int>> assigned_nodes;
  std::vector<int> num_local_threads;
  for (auto& kv: elem_->nodes) {
    assigned_nodes.push_back({kv.second.node.hostname, kv.second.node.id});
    num_local_threads.push_back(kv.second.num_local_threads);
  }
  CHECK(builder_);
  assigners_[spec->collection_id] = builder_();
  auto& assigner = assigners_[spec->collection_id];
  CHECK(assigner);
  int num_blocks =
      assigner->Load(spec->collection_id, spec->url, assigned_nodes, num_local_threads);
}

void BlockManager::FinishBlock(SArrayBinStream bin) {
  FinishedBlock block;
  bin >> block;
  LOG(INFO) << "[Scheduler] FinishBlock: " << block.DebugString();
  auto& assigner = assigners_[block.collection_id];
  bool done = assigner->FinishBlock(block);
  if (done) {
    auto blocks = assigner->GetFinishedBlocks();
    stored_blocks_[block.collection_id] = blocks;
    // construct the collection view
    std::vector<int> part_to_node(blocks.size());
    for (int i = 0; i < part_to_node.size(); ++i) {
      CHECK(blocks.find(i) != blocks.end()) << "unknown block id " << i;
      part_to_node[i] = blocks[i].node_id;
    }
    CollectionView cv;
    cv.collection_id = block.collection_id;
    cv.mapper = SimplePartToNodeMapper(part_to_node);
    cv.num_partition = cv.mapper.GetNumParts();
    elem_->collection_map->Insert(cv);

    // trigger InitWorkers
    SArrayBinStream bin;
    ToScheduler(ScheduleFlag::kInitWorkers, bin);
  }
}

void BlockManager::ToScheduler(ScheduleFlag flag, SArrayBinStream bin) {
  SArrayBinStream ctrl_bin;
  ctrl_bin << flag;
  Message msg;
  msg.meta.sender = -1;
  msg.meta.recver = 0;
  msg.meta.flag = Flag::kOthers;
  msg.AddData(ctrl_bin.ToSArray());
  msg.AddData(bin.ToSArray());
  elem_->sender->Send(std::move(msg));
}

} // namespace xyz
