#pragma once

namespace xyz {

enum class ScheduleFlag : char {
  kRegisterProgram,
  kInitWorkers,
  kInitWorkersReply,
  kRunMap,
  kRunSpeculativeMap,
  kFinishBlock,
  kLoadBlock,
  kDummy,
  kExit,
  kMapFinish,
  kJoinFinish,
  kDistribute,
  kFinishDistribute,
  kCheckPoint,
  kFinishCheckPoint,
  kLoadCheckPoint,
  kFinishLoadCheckPoint,
  kWritePartition,
  kFinishWritePartition,
  kController,
};
static const char *ScheduleFlagName[] = {"kRegisterProgram",
                                         "kInitWorker",
                                         "kInitWorkersReply",
                                         "kRunMap",
                                         "kRunSpeculativeMap",
                                         "kFinishBlock",
                                         "kLoadBlock",
                                         "kDummy",
                                         "kExit",
                                         "kMapFinish",
                                         "kJoinFinish",
                                         "kDistribute",
                                         "kFinishDistribute",
                                         "kCheckPoint",
                                         "kFinishCheckPoint",
                                         "kWritePartition",
                                         "kFinishWritePartition",
                                         "kController"
};

// currently workerinfo only has one field.
struct WorkerInfo {
  int num_local_threads;
};

struct ControllerMsg {
  enum class Flag : char {
    kSetup, kMap, kJoin, kFinish
  };
  Flag flag;
  int version;
  int node_id;
};

enum class ControllerFlag : char {
  kSetup,
  kStart,
  kFinishMap,
  kFinishJoin,
  kUpdateVersion,
  kReceiveJoin
};

} // namespace xyz
