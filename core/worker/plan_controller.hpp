#pragma once

#include <queue>
#include <chrono>
#include <mutex>
#include <atomic>

#include "core/worker/controller.hpp"
#include "core/worker/abstract_plan_controller.hpp"
#include "base/message.hpp"
#include "core/map_output/map_output_stream_store.hpp"

namespace xyz {

class PlanController : public AbstractPlanController {
 public:
  PlanController(Controller* controller);

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
    friend SArrayBinStream& operator<<(xyz::SArrayBinStream& stream, const VersionedJoinMeta& m) {
      stream << m.meta << m.bin;
      return stream;
    }
    friend SArrayBinStream& operator>>(xyz::SArrayBinStream& stream, VersionedJoinMeta& m) {
      stream >> m.meta >> m.bin;
      return stream;
    }
  };

  virtual void Setup(SpecWrapper spec) override;
  virtual void StartPlan() override;
  virtual void FinishMap(SArrayBinStream bin) override;
  virtual void FinishJoin(SArrayBinStream bin) override;
  virtual void UpdateVersion(SArrayBinStream bin) override;
  virtual void ReceiveJoin(Message msg) override;
  virtual void ReceiveFetchRequest(Message msg) override;
  virtual void FinishFetch(SArrayBinStream bin) override;
  virtual void FinishCheckpoint(SArrayBinStream bin) override;

  virtual void MigratePartition(Message msg) override;

  void TryRunSomeMaps();

  bool IsMapRunnable(int part_id);
  bool TryRunWaitingJoins(int part_id);

  void ReportFinishPart(ControllerMsg::Flag flag, int part_id, int version);

  void RunMap(int part_id, int version, std::shared_ptr<AbstractPartition>);
  void RunJoin(VersionedJoinMeta meta);
  void RunFetchRequest(VersionedJoinMeta fetch_meta);
  void Fetch(VersionedJoinMeta fetch_meta, int version);
  void SendMsgToScheduler(SArrayBinStream bin);
  void DisplayTime();

  // checkpoint
  bool TryCheckpoint(int part_id);

  bool IsJoinedBefore(const VersionedShuffleMeta& meta);

  // for migration
  void MigratePartitionStartMigrate(MigrateMeta);
  void MigratePartitionReceiveFlushAll(MigrateMeta);
  void MigratePartitionDest(Message msg);

  // for speculative execution
  void MigratePartitionStartMigrateMapOnly(MigrateMeta);
  void MigratePartitionReceiveMapOnly(Message);
 private:
  Controller* controller_;

  // TODO: to be setup
  int map_collection_id_;
  int join_collection_id_;
  int combine_;
  int fetch_collection_id_ = -1; 
  int plan_id_;
  int num_upstream_part_;
  int num_join_part_;
  int num_local_join_part_;
  int num_local_map_part_;
  SpecWrapper::Type type_;
  int checkpoint_interval_;

  int min_version_;
  int staleness_;
  int expected_num_iter_;

  // part -> version
  std::map<int, int> map_versions_;

  // part -> version
  std::map<int, int> join_versions_;
  // part -> version -> upstream_id (finished)
  std::map<int, std::map<int, std::set<int>>> join_tracker_;

  // for map_collection_id_ == join_collection_id_ only?
  // part, version
  std::map<int, std::map<int, std::deque<VersionedJoinMeta>>> pending_joins_;

  std::set<int> running_maps_;
  std::map<int, int> running_joins_;  // part_id -> upstream_id
  // std::map<int, std::set<int>> running_fetches_;// part_id, <upstream_part_id>
  std::map<int, int> running_fetches_;  // part_id, count
  // part -> join, some joins are waiting as there is a join writing that part
  std::map<int, std::deque<VersionedJoinMeta>> waiting_joins_;

  std::shared_ptr<Executor> fetch_executor_;

  std::mutex time_mu_;  
  std::map<int, std::tuple<std::chrono::system_clock::time_point, std::chrono::system_clock::time_point, std::chrono::system_clock::time_point>> map_time_;//part id
  std::map<int, std::map<int, std::pair<std::chrono::system_clock::time_point, std::chrono::system_clock::time_point>>> join_time_;//part id

  bool local_map_mode_ = true;  // TODO: turn it on
  MapOutputStreamStore stream_store_;

  int flush_all_count_ = 0;
  std::atomic<int> stop_joining_partition_{-1};
  struct MigrateData {
    int map_version;
    int join_version;
    std::map<int, std::deque<VersionedJoinMeta>> pending_joins;
    std::deque<VersionedJoinMeta> waiting_joins;
    std::map<int, std::set<int>> join_tracker;

    friend SArrayBinStream& operator<<(xyz::SArrayBinStream& stream, const MigrateData& d) {
      stream << d.map_version << d.join_version << d.pending_joins << d.waiting_joins << d.join_tracker;
      return stream;
    }
    friend SArrayBinStream& operator>>(xyz::SArrayBinStream& stream, MigrateData& d) {
      stream >> d.map_version >> d.join_version >> d.pending_joins >> d.waiting_joins >> d.join_tracker;
      return stream;
    }
    std::string DebugString() const {
      std::stringstream ss;
      ss << "map_version: " << map_version;
      ss << ", join_version: " << join_version;
      ss << ", pending_join size: " << pending_joins.size();
      ss << ", waiting_joins size: " << waiting_joins.size();
      ss << ", join_tracker size: " << join_tracker.size();
      return ss.str();
    }
  };
  std::vector<VersionedJoinMeta> buffered_requests_;
};

}  // namespace

