#pragma once

#include "base/actor.hpp"
#include "core/executor/executor.hpp"
#include "core/plan/function_store.hpp"
#include "core/partition/partition_tracker.hpp"

namespace xyz {

class JoinActor : public Actor {

public:
  JoinActor(int qid,
    std::shared_ptr<PartitionTracker> partition_tracker, 
    std::shared_ptr<Executor> executor,
    std::shared_ptr<FunctionStore> function_store)
  : Actor(qid), partition_tracker_(partition_tracker),
  executor_(executor), function_store_(function_store) {
    Start();
  }

  ~JoinActor() { Stop(); }

  virtual void Process(Message msg) override {
    SArrayBinStream bin_to_ctrl;
    SArrayBinStream bin_to_join;
    bin_to_ctrl.FromSArray(msg.data[0]);
    bin_to_join.FromSArray(msg.data[1]);
    int collection_id, partition_id, upstream_part_id, plan_id;
    bin_to_ctrl >> collection_id >> partition_id >> upstream_part_id >> plan_id;

    auto& func = function_store_->GetJoin(plan_id);
   
    JoinMeta join_meta;
    join_meta.collection_id = collection_id;
    join_meta.part_id = partition_id;
    join_meta.upstream_part_id = upstream_part_id;
    join_meta.func = [func, bin_to_join](std::shared_ptr<AbstractPartition> p) {
      func(p, bin_to_join);
    };
    partition_tracker_->RunJoin(join_meta);
  };

private:
  std::shared_ptr<PartitionTracker> partition_tracker_; 
  std::shared_ptr<Executor> executor_;
  std::shared_ptr<FunctionStore> function_store_;
};

} // namespace xyz
