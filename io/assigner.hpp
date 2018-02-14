#pragma once

#include <map>
#include <vector>
#include <set>
#include <sstream>
#include <memory>
#include <condition_variable>
#include <mutex>

#include "glog/logging.h"

#include "comm/abstract_sender.hpp"
#include "io/abstract_browser.hpp"
#include "base/message.hpp"
#include "base/actor.hpp"

namespace xyz {

class Assigner : public Actor {
 public:
  Assigner(int qid, std::shared_ptr<AbstractSender> sender, 
          std::shared_ptr<AbstractBrowser> browser)
      :Actor(qid), sender_(sender), browser_(browser) {
    Start();
  }
  ~Assigner() {
    Stop();
  }
  virtual void Process(Message msg);
  void InitBlocks(std::string url);
  int Load(std::string url, std::vector<std::pair<std::string, int>> slaves, int num_slots);
  void Wait();
  bool Assign(std::pair<std::string, int> slave);
  std::string DebugStringLocalityMap();
  std::string DebugStringBlocks();
  int GetNumBlocks();
 private:
  std::shared_ptr<AbstractBrowser> browser_;
  std::shared_ptr<AbstractSender> sender_;

  bool init_ = false;
  // host -> { local blocks}
  std::map<std::string, std::set<std::pair<std::string, size_t>>> locality_map_;
  // blocks locality information
  std::map<std::pair<std::string, size_t>, std::vector<std::string>> blocks_;
  int num_finished_ = 0;
  int expected_num_finished_ = 0;

  std::mutex mu_;
  std::condition_variable cond_;
};

}  // namespace xyz

