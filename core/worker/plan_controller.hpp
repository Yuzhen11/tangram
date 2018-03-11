#pragma once

#include <queue>

#include "core/worker/controller.hpp"
#include "core/worker/abstract_plan_controller.hpp"

namespace xyz {

class PlanController : public AbstractPlanController {
 public:
  PlanController(Controller* controller)
    : controller_(controller) {
  }

  ~PlanController() = default;

  struct VersionedShuffleMeta {
    int plan_id;  // TODO may not need plan_id, etc
    int collection_id;
    int part_id;
    int upstream_part_id;
    int version;
  
    std::string DebugString() const {
      std::stringstream ss;
      ss << "{";
      ss << " plan_id: " << plan_id;
      ss << ", collection_id: " << collection_id;
      ss << ", part_id: " << part_id;
      ss << ", upstream_part_id: " << upstream_part_id;
      ss << ", version: " << version;
      ss << " }";
      return ss.str();
    }
  };
  struct VersionedJoinMeta {
    VersionedShuffleMeta meta;
    SArrayBinStream bin;
  };

  virtual void Setup(SpecWrapper spec) override;
  virtual void StartPlan() override;
  virtual void FinishMap(SArrayBinStream bin) override;
  virtual void FinishJoin(SArrayBinStream bin) override;
  virtual void UpdateVersion(SArrayBinStream bin) override;
  virtual void ReceiveJoin(Message msg) override;

  void TryRunSomeMaps();

  bool IsMapRunnable(int part_id);
  bool TryRunWaitingJoins(int part_id);
  void TryUpdateMapVersion();
  void TryUpdateJoinVersion();
  void SendUpdateMapVersionToScheduler();
  void SendUpdateJoinVersionToScheduler();

  void RunMap(int part_id, int version);
  void RunJoin(VersionedJoinMeta meta);
  void SendMsgToScheduler(SArrayBinStream bin);
 private:
  Controller* controller_;

  // TODO: to be setup
  // TODO: now only support running 1 plan at a time
  int map_collection_id_;
  int join_collection_id_;
  int plan_id_;
  int num_upstream_part_;
  int num_local_join_part_;
  int num_local_map_part_;
  SpecWrapper::Type type_;

  int min_version_;
  int staleness_;

  // part -> version
  std::map<int, int> map_versions_;
  int min_map_version_;

  // part -> version
  std::map<int, int> join_versions_;
  int min_join_version_;
  // part -> version -> upstream_id (finished)
  std::map<int, std::map<int, std::set<int>>> join_tracker_;

  // for map_collection_id_ == join_collection_id_ only?
  // part, version
  std::map<int, std::map<int, std::deque<VersionedJoinMeta>>> pending_joins_;

  std::set<int> running_maps_;
  std::set<int> running_joins_;
  // part -> join, some joins are waiting as there is a join writing that part
  std::map<int, std::deque<VersionedJoinMeta>> waiting_joins_;
};

}  // namespace

