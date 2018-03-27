#pragma once

#include "core/scheduler/scheduler_elem.hpp"
#include "core/scheduler/checkpoint_loader.hpp"

#include "core/plan/spec_wrapper.hpp"

namespace xyz {

class CheckpointManager {
 public:
  CheckpointManager(std::shared_ptr<SchedulerElem> elem, 
          std::shared_ptr<CheckpointLoader> cp_loader)
      : elem_(elem), checkpoint_loader_(cp_loader) {}
  void Checkpoint(SpecWrapper s);
  void LoadCheckpoint(SpecWrapper s);
  void FinishCheckpoint(SArrayBinStream bin);
 private:
  std::map<int, int> checkpoint_reply_count_map;
  std::map<int, int> expected_checkpoint_reply_count_map;

  std::shared_ptr<SchedulerElem> elem_;
  std::shared_ptr<CheckpointLoader> checkpoint_loader_;

  // collection_id -> plan_id
  std::map<int,int> cid_pid_;
};

} // namespace xyz
