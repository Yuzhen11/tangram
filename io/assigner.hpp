#pragma once

#include <map>
#include <vector>
#include <set>
#include <sstream>
#include <memory>
#include <condition_variable>

#include "glog/logging.h"

#include "comm/abstract_sender.hpp"
#include "io/abstract_browser.hpp"
#include "base/message.hpp"
#include "io/meta.hpp"

namespace xyz {

class Assigner {
 public:
  Assigner(std::shared_ptr<AbstractSender> sender, 
          std::shared_ptr<AbstractBrowser> browser)
      :sender_(sender), browser_(browser) {
  }
  ~Assigner() = default;

  // public api:
  int Load(std::string url, std::vector<std::pair<std::string, int>> slaves, int num_slots);
  // return true if all blocks finish
  bool FinishBlock(FinishedBlock block);
  bool Done();
  std::map<int, std::tuple<std::string, size_t, int>> GetFinishedBlocks() const {
    return finished_blocks_;
  }

  void InitBlocks(std::string url);
  void Assign(std::pair<std::string, int> slave);
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
  std::map<int, std::tuple<std::string, size_t, int>> finished_blocks_;

  int block_id_ = 0;

  int num_finished_ = 0;
  int num_assigned_ = 0;
  int expected_num_finished_ = 0;
};

}  // namespace xyz

