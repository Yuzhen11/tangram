#pragma once

#include "core/scheduler/scheduler_elem.hpp"

#include "core/plan/spec_wrapper.hpp"

namespace xyz {

class CheckpointManager {
 public:
  CheckpointManager(std::shared_ptr<SchedulerElem> elem)
      : elem_(elem) {}
  void Checkpoint(SpecWrapper s);
  void LoadCheckpoint(SpecWrapper s);
  void FinishCheckpoint(SArrayBinStream bin);
  void FinishLoadCheckpoint(SArrayBinStream bin);

 private:
  std::map<int, int> checkpoint_reply_count_map;
  std::map<int, int> expected_checkpoint_reply_count_map;
  std::map<int, int> loadcheckpoint_reply_count_map;
  std::map<int, int> expected_loadcheckpoint_reply_count_map;
  std::shared_ptr<SchedulerElem> elem_;

  // collection_id -> plan_id
  std::map<int,int> cid_pid_;
};

} // namespace xyz
