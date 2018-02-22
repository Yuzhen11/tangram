#include "io/assigner.hpp"

namespace xyz {

bool Assigner::FinishBlock(FinishedBlock block) {
  CHECK(assigned_blocks_.find(block.block_id) != assigned_blocks_.end());
  auto url = assigned_blocks_[block.block_id].first;
  auto offset = assigned_blocks_[block.block_id].second;
  assigned_blocks_.erase(block.block_id);
  finished_blocks_.insert({block.block_id, {url, offset, block.node_id}});
  num_finished_ += 1;

  if (num_finished_ == expected_num_finished_) {
    return true;
  } else {
    std::pair<std::string, int> slave{block.hostname, block.qid};
    Assign(slave);
    return false;
  }
}

bool Assigner::Done() {
  return num_finished_ == expected_num_finished_;
}

void Assigner::InitBlocks(std::string url) {
  CHECK_NOTNULL(browser_);
  auto blocks = browser_->Browse(url);
  for (auto block : blocks) {
    std::pair<std::string, int> p{block.filename, block.offset};
    locality_map_[block.hostname].insert(p);
    blocks_[p].push_back(block.hostname);
  }
  init_ = true;
}

int Assigner::GetNumBlocks() {
  CHECK(init_);
  return blocks_.size();
}

// return num of blocks
int Assigner::Load(std::string url, std::vector<std::pair<std::string, int>> slaves, int num_slots) {
  InitBlocks(url);
  num_finished_ = 0;
  num_assigned_ = 0;
  expected_num_finished_ = blocks_.size();
  // Need to ensure that there is no message coming before Load().

  LOG(INFO) << "Assigning " << blocks_.size() << " paritition to "
      << slaves.size() << " slaves";
  // TODO: use more scheduling sophisticatic algorithm
  for (auto slave: slaves) {
    for (int i = 0; i < num_slots; ++ i) {
      Assign(slave);
    }
  }
  return expected_num_finished_;
}

void Assigner::Assign(std::pair<std::string, int> slave) {
  if (blocks_.empty()) {
    return;
  }

  std::pair<std::string, size_t> block;
  // find block according to locality if any
  if (locality_map_.find(slave.first) != locality_map_.end()
          && !locality_map_[slave.first].empty()) {
    block = *locality_map_[slave.first].begin();
  } else {
    block = blocks_.begin()->first;
  }

  // send
  SArrayBinStream ctrl_bin, bin;
  int type = 0;  // TODO
  ctrl_bin << type;
  AssignedBlock assigned_block;
  assigned_block.url = block.first;
  assigned_block.offset = block.second;
  assigned_block.id = block_id_ ++;
  assigned_block.collection_id = 0;  // TODO
  bin << assigned_block;
  Message msg;
  msg.meta.sender = 0;
  msg.meta.recver = slave.second;
  msg.AddData(ctrl_bin.ToSArray());
  msg.AddData(bin.ToSArray());
  sender_->Send(std::move(msg));
  VLOG(1) << "Assigining block: " << assigned_block.DebugString();

  // remove blocks info
  auto b = blocks_.find(block);
  CHECK(b != blocks_.end());
  auto locs = b->second;
  CHECK_GT(locs.size(), 0);
  for (auto loc : locs) {
    locality_map_[loc].erase(block);
  }
  blocks_.erase(block);
  num_assigned_ += 1;
  assigned_blocks_.insert({assigned_block.id, block});
}

std::string Assigner::DebugStringLocalityMap() {
  std::stringstream ss;
  for (auto& kv : locality_map_) {
    ss << "hostname: " << kv.first;
    ss << "\n{";
    for (auto p : kv.second) {
      ss << "<" << p.first << ", " << p.second << "> ";
    }
    ss << "}\n";
  }
  return ss.str();
}

std::string Assigner::DebugStringBlocks() {
  std::stringstream ss;
  for (auto& kv : blocks_) {
    ss << "block: <" << kv.first.first << ", " << kv.first.second << ">: ";
    for (auto& h : kv.second) {
      ss << h << ", ";
    }
    ss << "\n";
  }
  return ss.str();
}

std::string Assigner::DebugStringFinishedBlocks() {
  std::stringstream ss;
  ss << "finished block:\n";
  for (auto& kv : finished_blocks_) {
    ss << "block id: " << kv.first;
    ss << " <" << std::get<0>(kv.second) << ", " << std::get<1>(kv.second) 
        << "> on " << std::get<2>(kv.second);
    ss << "\n";
  }
  return ss.str();
}

}  // namespace xyz

