#pragma once

#include "base/actor.hpp"
#include "core/executor/executor.hpp"
#include "core/partition/partition_manager.hpp"
#include "core/plan/function_store.hpp"

namespace xyz {

class JoinActor : public Actor {

public:
  JoinActor(int qid,
    std::shared_ptr<PartitionManager> partition_manager,
    std::shared_ptr<Executor> executor,
    std::shared_ptr<FunctionStore> function_store)
  : Actor(qid), partition_manager_(partition_manager),
  executor_(executor), function_store_(function_store) {
    Start();
  }

  ~JoinActor() { Stop(); }

  virtual void Process(Message msg) override {
    SArrayBinStream bin_to_join;
    auto &func = function_store_->GetJoin(msg.meta.partition_id); //add plan_id to msg.meta? replace partition_id with plan_id
    auto part = partition_manager_->Get(msg.meta.collection_id, msg.meta.partition_id);
    bin_to_join.FromMsg(msg);
    executor_->Add([this, part, bin_to_join, func](){
      func(part->partition, bin_to_join);
    });
  };

private:
  std::shared_ptr<PartitionManager> partition_manager_;
  std::shared_ptr<Executor> executor_;
  std::shared_ptr<FunctionStore> function_store_;
};

} // namespace xyz