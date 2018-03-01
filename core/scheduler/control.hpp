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
};
static const char* ScheduleFlagName[] = {
  "kRegisterProgram",
  "kInitWorker",
  "kInitWorkersReply",
  "kRunMap",
  "kRunSpeculativeMap",
  "kFinishBlock",
  "kLoadBlock",
  "kDummy",
  "kExit"
  "kMapFinish",
  "kJoinFinish"
  "kDistribute",
  "kFinishDistribute",
};

}  // namespace xyz

