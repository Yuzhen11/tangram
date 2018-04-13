#pragma once

#include <sstream>
#include <string>
#include <glog/logging.h>
namespace xyz {

enum class ScheduleFlag : char {
  kRegisterProgram,
  kFinishBlock,
  kLoadBlock,
  kDummy,
  kExit,
  kDistribute,
  kFinishDistribute,
  kCheckpoint,
  kFinishCheckpoint,
  kLoadCheckpoint,
  kFinishLoadCheckpoint,
  kWritePartition,
  kFinishWritePartition,
  kControl,
  kFinishPlan,
  kUpdateCollection,
  kUpdateCollectionReply,
  kRecovery,
};

static const char *ScheduleFlagName[] = {
  "kRegisterProgram",
  "kFinishBlock",
  "kLoadBlock",
  "kDummy",
  "kExit",
  "kDistribute",
  "kFinishDistribute",
  "kCheckpoint",
  "kFinishCheckpoint",
  "kLoadCheckpoint",
  "kFinishLoadCheckpoint",
  "kWritePartition",
  "kFinishWritePartition",
  "kControl",
  "kFinishPlan",
  "kUpdateCollection",
  "kUpdateCollectionReply",
  "kRecovery",
  "kFinishRecovery"
};

enum class FetcherFlag : char{
  kFetchObjsReply,
  kFetchPartRequest,
  kFetchPartReplyLocal,
  kFetchPartReplyRemote,
};

// currently workerinfo only has one field.
struct WorkerInfo {
  int num_local_threads;
};

/*
 * The message sent between the control_manager and the plan_controller.
 */
struct ControllerMsg {
  enum class Flag : char {
    kSetup, kMap, kJoin, kFinish, kFinishMigrate
  };
  static constexpr const char* FlagName[] = {
    "kSetup", "kMap", "kJoin", "kFinish", "kFinishMigrate"
  };
  Flag flag;
  int version;
  int node_id;
  int plan_id;
  int part_id;
  std::string DebugString() const {
    std::stringstream ss;
    ss << "flag: " << FlagName[static_cast<int>(flag)];
    ss << ", version: " << version;
    ss << ", node_id: " << node_id;
    ss << ", plan_id: " << plan_id;
    ss << ", part_id: " << part_id;
    return ss.str();
  }
};

enum class ControllerFlag : char {
  kSetup,
  kStart,
  kFinishMap,
  kFinishJoin,
  kUpdateVersion,
  kReceiveJoin,
  kFetchRequest,
  kFinishFetch,
  kFinishCheckpoint,
  kMigratePartition,
  kTerminatePlan,
  kFinishLoadWith,
  kReassignMap,  // no partition lost during machine failure, reassign the map partitions
};

static const char *ControllerFlagName[] = {
  "kSetup",
  "kStart",
  "kFinishMap",
  "kFinishJoin",
  "kUpdateVersion",
  "kReceiveJoin",
  "kFetchRequest",
  "kFinishFetch",
  "kFinishCheckpoint",
  "kMigratePartition",
  "kTerminatePlan",
  "kFinishLoadWith",
  "kReassignMap",
};

struct FetchMeta {
  int plan_id; 
  int upstream_part_id;
  int collection_id;
  int partition_id; 
  int version;
  bool local_mode;
  std::string DebugString() const {
    std::stringstream ss;
    ss << "plan_id: " << plan_id;
    ss << ", upstream_part_id: " << upstream_part_id;
    ss << ", collection_id: " << collection_id;
    ss << ", partition_id: " << partition_id;
    ss << ", version: " << version;
    ss << ", local_mode: " << (local_mode ? "true":"false");
    return ss.str();
  }
};

struct MigrateMeta {
  enum class MigrateFlag {
    kStartMigrate,
    kFlushAll,
    kDest,
    kStartMigrateMapOnly,
    kReceiveMapOnly,
  };
  static constexpr const char* FlagName[] = {
    "kStartMigrate", 
    "kFlushAll",
    "kDest",
    "kStartMigrateMapOnly",
    "kReceiveMapOnly"
  };
  MigrateFlag flag;
  int plan_id;
  int collection_id;
  int partition_id;
  int from_id;
  int to_id;  // the node id
  int num_nodes;
  std::string DebugString() const {
    std::stringstream ss;
    ss << "flag: " << FlagName[static_cast<int>(flag)];
    ss << ", plan_id: " << plan_id;
    ss << ", collection_id: " << collection_id;
    ss << ", partition_id: " << partition_id;
    ss << ", from_id: " << from_id;
    ss << ", to_id: " << to_id;
    ss << ", num_nodes: " << num_nodes;
    return ss.str();
  }
};

namespace {

// shared by checkpoint_loader, checkpoint_manager, plan_controller
std::string GetCheckpointUrl(std::string url, 
        int collection_id, int partition_id) {
  std::string dest_url = url + 
      "/c" + std::to_string(collection_id) + "-p" + std::to_string(partition_id);
  return dest_url;
}

const int kRecoverMagic = 10000;
bool IsRecoverPlan(int id) {
  return id >= kRecoverMagic ? true:false;
}
int GetRecoverPlanId(int id) {
  CHECK(!IsRecoverPlan(id)) << "cannot recover recovery plan";
  return id + kRecoverMagic;
}

int GetRealId(int id) {
  return id >= kRecoverMagic ? id - kRecoverMagic : id;
}


}  // namespace

} // namespace xyz
