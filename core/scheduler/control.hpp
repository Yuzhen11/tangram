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
  kWritePartition,
  kFinishWritePartition,
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
                                         "kFinishWritePartition"
};

// currently workerinfo only has one field.
struct WorkerInfo {
  int num_local_threads;
};

} // namespace xyz
