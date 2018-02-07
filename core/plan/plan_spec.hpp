#pragma once

namespace xyz {

struct PlanSpec {
  int plan_id;
  int map_collection_id;
  int join_collection_id;

  int with_collection_id;
  PlanSpec(int pid, int mid, int jid, int wid)
      : plan_id(pid), map_collection_id(mid), join_collection_id(jid), with_collection_id(wid)
  {}
};

}  // namespace xyz
