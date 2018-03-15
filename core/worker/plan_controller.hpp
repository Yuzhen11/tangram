#pragma once

#include <queue>
#include <chrono>
#include <mutex>

#include "core/worker/controller.hpp"
#include "core/worker/abstract_plan_controller.hpp"
#include "base/message.hpp"

namespace xyz {

class PlanController : public AbstractPlanController {
 public:
  PlanController(Controller* controller)
    : controller_(controller) {
    fetch_executor_ = std::make_shared<Executor>(1);
  }

  ~PlanController() = default;

  struct VersionedShuffleMeta {
    int plan_id;  // TODO may not need plan_id, etc
    int collection_id;
    int part_id;
    int upstream_part_id;
    int version;
    bool is_fetch = false;
    int sender = -1;
    int recver = -1;
    bool local_mode = false;

    std::string DebugString() const {
      std::stringstream ss;
      ss << "{";
      ss << " plan_id: " << plan_id;
      ss << ", collection_id: " << collection_id;
      ss << ", part_id: " << part_id;
      ss << ", upstream_part_id: " << upstream_part_id;
      ss << ", version: " << version;
      ss << ", is fetch: " << is_fetch;
      ss << ", sender: " << sender;
      ss << ", recver: " << recver;
      ss << ", local_mode: " << local_mode;
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
  virtual void ReceiveFetchRequest(Message msg) override;
  virtual void FinishFetch(SArrayBinStream bin) override;

  void TryRunSomeMaps();

  bool IsMapRunnable(int part_id);
  bool TryRunWaitingJoins(int part_id);
  void TryUpdateMapVersion();
  void TryUpdateJoinVersion();
  void SendUpdateMapVersionToScheduler();
  void SendUpdateJoinVersionToScheduler();

  void RunMap(int part_id, int version);
  void RunJoin(VersionedJoinMeta meta);
  void RunFetchRequest(VersionedJoinMeta fetch_meta);
  void Fetch(VersionedJoinMeta fetch_meta);
  void SendMsgToScheduler(SArrayBinStream bin);
  void DisplayTime();

 private:
  Controller* controller_;

  // TODO: to be setup
  // TODO: now only support running 1 plan at a time
  int map_collection_id_;
  int join_collection_id_;
  int fetch_collection_id_ = -1; 
  int plan_id_;
  int num_upstream_part_;
  int num_local_join_part_;
  int num_local_map_part_;
  SpecWrapper::Type type_;

  int min_version_;
  int staleness_;
  int expected_num_iter_;

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
  std::map<int, std::set<int>> running_fetches_;// part_id, <upstream_part_id>
  // part -> join, some joins are waiting as there is a join writing that part
  std::map<int, std::deque<VersionedJoinMeta>> waiting_joins_;

  std::shared_ptr<Executor> fetch_executor_;

  std::mutex time_mu_;  
  std::map<int, std::tuple<std::chrono::system_clock::time_point, std::chrono::system_clock::time_point, std::chrono::system_clock::time_point>> map_time_;//part id
  std::map<int, std::map<int, std::pair<std::chrono::system_clock::time_point, std::chrono::system_clock::time_point>>> join_time_;//part id
};

}  // namespace

