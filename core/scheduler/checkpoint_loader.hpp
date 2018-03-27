#pragma once

#include <functional>

#include "core/scheduler/scheduler_elem.hpp"

namespace xyz {


class CheckpointLoader {
 public:
  CheckpointLoader(std::shared_ptr<SchedulerElem> elem)
      : elem_(elem) {}

  // non thread-safe
  void LoadCheckpoint(int cid, std::string url,
        std::function<void()> f);
  void FinishLoadCheckpoint(SArrayBinStream bin);
 private:
  std::shared_ptr<SchedulerElem> elem_;

  std::map<int, int> loadcheckpoint_reply_count_map_;
  std::map<int, int> expected_loadcheckpoint_reply_count_map_;
  std::map<int, std::function<void()>> callbacks_;
};

} // namespace xyz
