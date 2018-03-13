#pragma once

#include "core/scheduler/scheduler_elem.hpp"

#include "core/plan/spec_wrapper.hpp"

namespace xyz {

class WriteManager {
 public:
  WriteManager(std::shared_ptr<SchedulerElem> elem)
      : elem_(elem) {}
  void Write(SpecWrapper spec);
  void FinishWritePartition(SArrayBinStream bin);

 private:
  std::map<int, int> reply_count_map;
  std::map<int, int> expected_reply_count_map;
  std::shared_ptr<SchedulerElem> elem_;

};

} // namespace xyz