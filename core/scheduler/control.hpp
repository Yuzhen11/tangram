namespace xyz {

enum class ScheduleFlag : char {
  kRegisterPlan,
  kInitWorkers,
  kInitWorkersReply,
  kRunMap,
  kRunSpeculativeMap,
  kFinishBlock
};
static const char* ScheduleFlagName[] = {
  "kRegisterPlan",
  "kInitWorker",
  "kInitWorkersReply",
  "kRunMap",
  "kRunSpeculativeMap",
  "kFinishBlock"
};

}  // namespace xyz

