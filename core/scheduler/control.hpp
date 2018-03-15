#pragma once

#include <sstream>
#include <string>

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
  kUpdateCollectionReply
};

static const char *ScheduleFlagName[] = {"kRegisterProgram",
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
                                         "kUpdateCollectionReply"
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

struct ControllerMsg {
  enum class Flag : char {
    kSetup, kMap, kJoin, kFinish
  };
  static constexpr const char* FlagName[] = {
    "kSetup", "kMap", "kJoin", "kFinish"
  };
  Flag flag;
  int version;
  int node_id;
  int plan_id;
  std::string DebugString() const {
    std::stringstream ss;
    ss << "flag: " << FlagName[static_cast<int>(flag)];
    ss << ", version: " << version;
    ss << ", node_id: " << node_id;
    ss << ", plan_id: " << plan_id;
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
    ss << ", local_mode: " << local_mode;
    return ss.str();
  }
};

} // namespace xyz
