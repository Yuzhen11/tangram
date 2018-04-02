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

  // parts: the partition ids that need to load
  void LoadCheckpointPartial(int cid, std::string url,
        std::vector<int> parts,
        std::function<void()> f);

  void SendLoadCommand(int cid, int part_id, int node_id, std::string url);

  static std::string GetCheckpointUrl(std::string url, int collection_id, int partition_id);
 private:
  std::shared_ptr<SchedulerElem> elem_;

  std::map<int, int> loadcheckpoint_reply_count_map_;
  std::map<int, int> expected_loadcheckpoint_reply_count_map_;
  std::map<int, std::function<void()>> callbacks_;
};

} // namespace xyz
