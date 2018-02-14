#include "io/assigner.hpp"

namespace xyz {

void Assigner::Process(Message msg) {
  CHECK_EQ(init_, true);
  SArrayBinStream ctrl_bin;
  SArrayBinStream bin;
  CHECK_EQ(msg.data.size(), 2);
  ctrl_bin.FromSArray(msg.data[0]);
  bin.FromSArray(msg.data[1]);
  int type;
  ctrl_bin >> type;
  if (type == 0) {  // TODO
    std::pair<std::string, int> slave;
    bin >> slave;
    Assign(slave);
  }
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
  expected_num_finished_ = slaves.size() * num_slots;
  int num_blocks = blocks_.size();
  // Need to ensure that there is no message coming before Load().

  LOG(INFO) << "Assigning " << blocks_.size() << " paritition to "
      << slaves.size() << " slaves";
  for (auto slave: slaves) {
    for (int i = 0; i < num_slots; ++ i) {
      Assign(slave);
    }
  }
  return num_blocks;
}

void Assigner::Wait() {
  std::unique_lock<std::mutex> lk(mu_);
  cond_.wait(lk, [this]() {
    return blocks_.empty() && num_finished_ == expected_num_finished_;
  });
}

bool Assigner::Assign(std::pair<std::string, int> slave) {
  std::unique_lock<std::mutex> lc(mu_);
  if (blocks_.empty()) {
    num_finished_ += 1;
    VLOG(1) << "finish " << num_finished_ << " " << slave.first << " " << slave.second;
    if (num_finished_ == expected_num_finished_)
      cond_.notify_one();
    return false;
  }
  std::pair<std::string, int> block;
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
  bin << block.first << block.second;  // TODO more info
  Message msg;
  msg.meta.sender = 0;
  msg.meta.recver = slave.second;
  msg.AddData(ctrl_bin.ToSArray());
  msg.AddData(bin.ToSArray());
  sender_->Send(std::move(msg));
  VLOG(1) << "Assigining block <" << block.first << "," << block.second
      << "> to node: <" << slave.first << "," << slave.second << ">";

  // remove blocks info
  auto b = blocks_.find(block);
  CHECK(b != blocks_.end());
  auto locs = b->second;
  CHECK_GT(locs.size(), 0);
  for (auto loc : locs) {
    locality_map_[loc].erase(block);
  }
  blocks_.erase(block);

  return true;
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

}  // namespace xyz

