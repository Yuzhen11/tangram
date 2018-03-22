#pragma once

#include "core/scheduler/scheduler_elem.hpp"

#include "core/plan/spec_wrapper.hpp"

namespace xyz {

class RecoverManager {
 public:
  RecoverManager(std::shared_ptr<SchedulerElem> elem)
      : elem_(elem) {}
  // the scheduler will call recover to recover all the collections
  // (both mutable and immutable), when all the collections are 
  // reconstructed, send a message to scheduler to resume the computation
  void Recover(std::set<int> mutable_collection,
                std::set<int> immutable_collection,
                std::set<int> dead_nodes);

  std::vector<std::pair<int, int>> ReplaceDeadnodesAndReturnUpdated(
          int cid, std::set<int> dead_nodes);
 private:
  std::shared_ptr<SchedulerElem> elem_;
};

} // namespace xyz

