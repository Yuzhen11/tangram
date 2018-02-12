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
    SArrayBinStream bin_to_ctrl;
    SArrayBinStream bin_to_join;
    bin_to_ctrl.FromSArray(msg.data[0]);
    bin_to_join.FromSArray(msg.data[1]);
    int collection_id, partition_id;
    bin_to_ctrl >> collection_id >> partition_id;
    auto &func = function_store_->GetJoin(partition_id); //add plan_id to msg.meta? replace partition_id with plan_id
    auto part = partition_manager_->Get(collection_id, partition_id);
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
