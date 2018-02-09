namespace xyz {

enum class ScheduleFlag : char {
  kRegisterPlan,
  kInitWorkers,
  kInitWorkersReply,
  kRunMap,
  kRunSpeculativeMap
};
static const char* ScheduleFlagName[] = {
  "kRegisterPlan",
  "kInitWorker",
  "kInitWorkersReply",
  "kRunMap",
  "kRunSpeculativeMap"
};

}  // namespace xyz

