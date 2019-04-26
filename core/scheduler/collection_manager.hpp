#pragma once

#include <functional>

#include "core/scheduler/scheduler_elem.hpp"

#include "core/plan/spec_wrapper.hpp"

namespace xyz {

class CollectionManager {
 public:
  CollectionManager(std::shared_ptr<SchedulerElem> elem)
      : elem_(elem) {}

  // non thread-safe
  // update the collection map at each worker
  void Update(int collection_id, std::function<void()> f);

  void FinishUpdate(SArrayBinStream bin);

 private:
  std::shared_ptr<SchedulerElem> elem_;

  // collection_id -> replied_node
  std::map<int, std::set<int>> received_replies_;
  std::map<int, std::function<void()>> callbacks_;
};

} // namespace xyz

