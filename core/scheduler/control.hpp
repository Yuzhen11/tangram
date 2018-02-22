namespace xyz {

enum class ScheduleFlag : char {
  kRegisterPlan,
  kInitWorkers,
  kInitWorkersReply,
  kRunMap,
  kRunSpeculativeMap,
  kFinishBlock,
  kLoadBlock
};
static const char* ScheduleFlagName[] = {
  "kRegisterPlan",
  "kInitWorker",
  "kInitWorkersReply",
  "kRunMap",
  "kRunSpeculativeMap",
  "kFinishBlock",
  "kLoadBlock"
};

}  // namespace xyz

