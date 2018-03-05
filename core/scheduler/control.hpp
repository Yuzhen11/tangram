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

} // namespace xyz
