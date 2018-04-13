#pragma once

#include "core/scheduler/scheduler_elem.hpp"
#include "core/scheduler/checkpoint_loader.hpp"
#include "core/scheduler/collection_manager.hpp"
#include "core/scheduler/collection_status.hpp"
#include "core/plan/spec_wrapper.hpp"

#include <chrono>

namespace xyz {

class RecoverManager {
 public:
  RecoverManager(std::shared_ptr<SchedulerElem> elem, std::shared_ptr<CollectionManager> collection_manager,
    std::shared_ptr<CheckpointLoader> checkpoint_loader)
      : elem_(elem), collection_manager_(collection_manager), checkpoint_loader_(checkpoint_loader) {}

  // <int, std::string>: <collection_id, checkpoint>
  void Recover(std::set<int> dead_nodes,
      std::vector<std::pair<int, std::string>> writes, 
      std::vector<std::pair<int, std::string>> reads,
      std::function<void()> callback);

  std::vector<int> ReplaceDeadnodesAndReturnUpdated(
          int cid, std::set<int> dead_nodes);

  enum class Type {
    LoadCheckpoint, UpdateCollectionMap
  };
  void RecoverDoneForACollection(int cid, RecoverManager::Type type);
 private:
  std::shared_ptr<SchedulerElem> elem_;
  std::shared_ptr<CollectionManager> collection_manager_;
  std::shared_ptr<CheckpointLoader> checkpoint_loader_;

  std::set<int> recovering_collections_;
  std::set<int> updating_collections_;

  std::chrono::system_clock::time_point start_time_;
  bool started_ = false;
  std::function<void()> callback_;
};

} // namespace xyz

