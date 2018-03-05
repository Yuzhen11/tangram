#pragma once

#include <condition_variable>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <vector>

#include "glog/logging.h"

#include "base/message.hpp"
#include "comm/abstract_sender.hpp"
#include "io/abstract_browser.hpp"
#include "io/meta.hpp"

namespace xyz {

class Assigner {
public:
  Assigner(std::shared_ptr<AbstractSender> sender,
           std::shared_ptr<AbstractBrowser> browser)
      : sender_(sender), browser_(browser) {}
  ~Assigner() = default;

  // public api:
  // non threadsafe
  int Load(int collection_id, std::string url,
           std::vector<std::pair<std::string, int>> slaves, 
           std::vector<int> num_local_threads);

  // return true if all blocks finish
  bool FinishBlock(FinishedBlock block);
  bool Done();
  std::map<int, StoredBlock> GetFinishedBlocks() const {
    return finished_blocks_;
  }

  void InitBlocks(std::string url);
  bool Assign(int collection_id, std::pair<std::string, int> slave);
  std::string DebugStringLocalityMap();
  std::string DebugStringBlocks();
  std::string DebugStringFinishedBlocks();
  int GetNumBlocks();

private:
  std::shared_ptr<AbstractBrowser> browser_;
  std::shared_ptr<AbstractSender> sender_;

  bool init_ = false;
  // host -> { local blocks}
  std::map<std::string, std::set<std::pair<std::string, size_t>>> locality_map_;
  // blocks locality information
  std::map<std::pair<std::string, size_t>, std::vector<std::string>> blocks_;

  // assigned blocks
  std::map<int, std::pair<std::string, size_t>> assigned_blocks_;

  // finished blocks
  // part_id/block_id: <url, offset, node_id>
  std::map<int, StoredBlock> finished_blocks_;

  int block_id_ = 0;

  int num_finished_ = 0;
  int num_assigned_ = 0;
  int expected_num_finished_ = 0;
};

} // namespace xyz
