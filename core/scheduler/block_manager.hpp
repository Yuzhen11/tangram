#pragma once

#include "core/scheduler/scheduler_elem.hpp"
#include "core/scheduler/control.hpp"

#include "io/assigner.hpp"
#include "io/meta.hpp"

#include "core/plan/spec_wrapper.hpp"

#include "glog/logging.h"

namespace xyz {

class BlockManager {
 public:
  BlockManager(std::shared_ptr<SchedulerElem> elem, 
          std::function<std::shared_ptr<Assigner>()> builder);
  void Load(SpecWrapper spec_wrapper);
  void FinishBlock(SArrayBinStream bin);

 private:
  std::map<int, std::shared_ptr<Assigner>> assigners_;  // collection_id -> assigner
  std::function<std::shared_ptr<Assigner>()> builder_;
  // collection_id, part_id, <url, offset, node_id>
  std::map<int, std::map<int, StoredBlock>> stored_blocks_;

  std::map<int,int> cid_pid_;  // collection_id -> plan_id

  std::shared_ptr<SchedulerElem> elem_;
};

} // namespace xyz
