#pragma once

#include <queue>
#include <chrono>
#include <mutex>
#include <atomic>

#include "core/worker/controller.hpp"
#include "core/worker/abstract_plan_controller.hpp"
#include "base/message.hpp"
#include "core/map_output/map_output_stream_store.hpp"
#include "core/worker/delayed_combiner.hpp"

namespace xyz {

class DelayedCombiner;
class PlanController : public AbstractPlanController {
 public:
  PlanController(Controller* controller);

  ~PlanController();

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
    std::vector<int> ext_upstream_part_ids;

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
      ss << ", ext_upstream_part_ids size: " << ext_upstream_part_ids.size();
      for (int id : ext_upstream_part_ids) ss << "; " << id;
      ss << " }";
      return ss.str();
    }
    friend SArrayBinStream& operator<<(xyz::SArrayBinStream& stream, const VersionedShuffleMeta& m) {
      stream << m.plan_id << m.collection_id << m.part_id << m.upstream_part_id << m.version
             << m.is_fetch << m.sender << m.recver << m.local_mode << m.ext_upstream_part_ids;
      return stream;
    }
    friend SArrayBinStream& operator>>(xyz::SArrayBinStream& stream, VersionedShuffleMeta& m) {
      stream >> m.plan_id >> m.collection_id >> m.part_id >> m.upstream_part_id >> m.version
             >> m.is_fetch >> m.sender >> m.recver >> m.local_mode >> m.ext_upstream_part_ids;
      return stream;
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
  virtual void DisplayTime() override;

  virtual void MigratePartition(Message msg) override;
  virtual void ReassignMap(SArrayBinStream bin) override;

  void TryRunSomeMaps();

  bool IsMapRunnable(int part_id);
  bool TryRunWaitingJoins(int part_id);

  void ReportFinishPart(ControllerMsg::Flag flag, int part_id, int version);

  void RunMap(int part_id, int version, std::shared_ptr<AbstractPartition>);
  void RunJoin(VersionedJoinMeta meta);
  void RunFetchRequest(VersionedJoinMeta fetch_meta);
  void Fetch(VersionedJoinMeta fetch_meta, int version);

  // checkpoint
  bool TryCheckpoint(int part_id);

  bool IsJoinedBefore(const VersionedShuffleMeta& meta);

  // for migration
  void MigratePartitionStartMigrate(MigrateMeta);
  void MigratePartitionReceiveFlushAll(MigrateMeta);
  void MigratePartitionDest(Message msg);
  void FinishLoadWith(SArrayBinStream bin) override;//for load cp in migration

  // for speculative execution
  void MigratePartitionStartMigrateMapOnly(MigrateMeta);
  void MigratePartitionReceiveMapOnly(Message);
 private:
  Controller* controller_;

  // TODO: to be setup
  int map_collection_id_;
  int update_collection_id_;
  int fetch_collection_id_ = -1; 
  int plan_id_;
  int num_upstream_part_;
  int num_update_part_;
  int num_local_update_part_;
  int num_local_map_part_;
  SpecWrapper::Type type_;
  int checkpoint_interval_;
  std::string checkpoint_path_;

  int min_version_;
  int staleness_;
  int expected_num_iter_;

  // part -> version
  std::unordered_map<int, int> map_versions_;

  // part -> version
  std::unordered_map<int, int> update_versions_;
  // part -> version -> upstream_id (finished)
  std::unordered_map<int, std::unordered_map<int, std::set<int>>> update_tracker_;
  std::vector<int> update_tracker_size_;
  void CalcJoinTrackerSize();
  void ShowJoinTrackerSize();

  // for map_collection_id_ == update_collection_id_ only?
  // part, version
  std::unordered_map<int, std::unordered_map<int, std::deque<VersionedJoinMeta>>> pending_updates_;

  std::set<int> running_maps_;
  std::unordered_map<int, int> running_updates_;  // part_id -> upstream_id
  // std::map<int, std::set<int>> running_fetches_;// part_id, <upstream_part_id>
  std::unordered_map<int, int> running_fetches_;  // part_id, count
  // part -> update, some updates are waiting as there is a update writing that part
  std::unordered_map<int, std::deque<VersionedJoinMeta>> waiting_updates_;

  std::shared_ptr<Executor> fetch_executor_;
  std::shared_ptr<Executor> map_executor_;

  std::mutex time_mu_;  
  std::map<int, std::tuple<std::chrono::system_clock::time_point, std::chrono::system_clock::time_point, std::chrono::system_clock::time_point>> map_time_;//part id
  std::map<int, std::map<int, std::pair<std::chrono::system_clock::time_point, std::chrono::system_clock::time_point>>> update_time_;//part id

  bool local_map_mode_ = true;  // TODO: turn it on
  MapOutputStreamStore stream_store_;

  std::map<int, int> flush_all_count_; //migrate part id, flush all count
  std::map<int, bool> stop_updateing_partitions_; //migrate part id, flag indicating whether it could be erased
  std::mutex stop_updateing_partitions_mu_;

  std::map<int, bool> load_finished_;

  struct MigrateData {
    int map_version;
    int update_version;
    std::unordered_map<int, std::deque<VersionedJoinMeta>> pending_updates;
    std::deque<VersionedJoinMeta> waiting_updates;
    std::unordered_map<int, std::set<int>> update_tracker;

    friend SArrayBinStream& operator<<(xyz::SArrayBinStream& stream, const MigrateData& d) {
      stream << d.map_version << d.update_version << d.pending_updates << d.waiting_updates << d.update_tracker;
      return stream;
    }
    friend SArrayBinStream& operator>>(xyz::SArrayBinStream& stream, MigrateData& d) {
      stream >> d.map_version >> d.update_version >> d.pending_updates >> d.waiting_updates >> d.update_tracker;
      return stream;
    }
    std::string DebugString() const {
      std::stringstream ss;
      ss << "map_version: " << map_version;
      ss << ", update_version: " << update_version;
      ss << ", pending_update size: " << pending_updates.size();
      ss << ", waiting_updates size: " << waiting_updates.size();
      ss << ", update_tracker size: " << update_tracker.size();
      return ss.str();
    }
  };
  std::map<int, std::vector<VersionedJoinMeta>> buffered_requests_; //part_id -> requests
  friend class DelayedCombiner;
  std::shared_ptr<DelayedCombiner> delayed_combiner_;

  std::mutex migrate_mu_;
};

}  // namespace

