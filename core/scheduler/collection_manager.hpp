#pragma once

#include "core/scheduler/scheduler_elem.hpp"

#include "core/plan/spec_wrapper.hpp"

namespace xyz {

class CollectionManager {
 public:
  CollectionManager(std::shared_ptr<SchedulerElem> elem)
      : elem_(elem) {}

  // update the collection map at each worker
  void Update(SArrayBinStream bin);
  void FinishUpdate(SArrayBinStream bin);

 private:
  std::shared_ptr<SchedulerElem> elem_;

  // {plan_id, collection_id} -> replied_node
  std::map<std::pair<int,int>, std::set<int>> received_replies_;
};

} // namespace xyz

