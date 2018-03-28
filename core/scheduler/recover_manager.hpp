#pragma once

#include "core/scheduler/scheduler_elem.hpp"
#include "core/scheduler/checkpoint_loader.hpp"
#include "core/scheduler/collection_manager.hpp"
#include "core/scheduler/collection_status.hpp"
#include "core/plan/spec_wrapper.hpp"

namespace xyz {

class RecoverManager {
 public:
  RecoverManager(std::shared_ptr<SchedulerElem> elem, std::shared_ptr<CollectionManager> collection_manager,
    std::shared_ptr<CheckpointLoader> checkpoint_loader, std::shared_ptr<CollectionStatus> collection_status)
      : elem_(elem), collection_manager_(collection_manager), checkpoint_loader_(checkpoint_loader), collection_status_(collection_status) {}

  void Recover(std::set<int> dead_nodes);
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
  std::shared_ptr<CollectionStatus> collection_status_;

  std::set<int> recovering_collections_;
  std::set<int> updating_collections_;
};

} // namespace xyz

