#pragma once

#include "base/actor.hpp"
#include "core/executor/executor.hpp"
#include "core/plan/function_store.hpp"
#include "core/partition/partition_tracker.hpp"
#include "core/shuffle_meta.hpp"

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
    SArrayBinStream ctrl_bin, bin;
    SArrayBinStream bin_to_join;
    ctrl_bin.FromSArray(msg.data[0]);
    bin.FromSArray(msg.data[1]);
    ShuffleMeta meta;
    ctrl_bin >> meta; 

    auto& func = function_store_->GetJoin(meta.plan_id);
   
    JoinMeta join_meta;
    join_meta.part_id = meta.part_id;
    join_meta.upstream_part_id = meta.upstream_part_id;
    join_meta.func = [func, bin](std::shared_ptr<AbstractPartition> p) {
      func(p, bin);
    };
    partition_tracker_->RunJoin(join_meta);
  };

private:
  std::shared_ptr<PartitionTracker> partition_tracker_; 
  std::shared_ptr<Executor> executor_;
  std::shared_ptr<FunctionStore> function_store_;
};

} // namespace xyz
