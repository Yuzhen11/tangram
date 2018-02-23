namespace xyz {

enum class ScheduleFlag : char {
  kStart,
  kRegisterProgram,
  kInitWorkers,
  kInitWorkersReply,
  kRunMap,
  kRunSpeculativeMap,
  kFinishBlock,
  kLoadBlock,
  kExit
};
static const char* ScheduleFlagName[] = {
  "kStart",
  "kRegisterProgram",
  "kInitWorker",
  "kInitWorkersReply",
  "kRunMap",
  "kRunSpeculativeMap",
  "kFinishBlock",
  "kLoadBlock",
  "kExit"
};

}  // namespace xyz

