#pragma once

#include <unordered_map>

#include "base/actor.hpp"
#include "base/threadsafe_queue.hpp"
#include "core/index/abstract_part_to_node_mapper.hpp"
#include "core/index/simple_part_to_node_mapper.hpp"
#include "comm/abstract_sender.hpp"

namespace xyz {

class PartToNodeManager : public Actor {
 public:
  PartToNodeManager(int qid, std::shared_ptr<AbstractSender> sender): 
      Actor(qid), sender_(sender) {
    Start();
  }
  virtual ~PartToNodeManager() override {
    Stop();
  }
  virtual void Process(Message msg) override;

  void Initialize(Message msg);
 private:
  std::unordered_map<int, std::shared_ptr<AbstractPartToNodeMapper>> map_;
  std::shared_ptr<AbstractSender> sender_;
};

}  // namespace xyz

