#pragma once

#include <sstream>

#include "base/sarray_binstream.hpp"

namespace xyz {

struct PlanSpec {
  int plan_id;
  int map_collection_id;
  int join_collection_id;
  int cur_iter = 0;
  int num_iter = 1;

  int with_collection_id = -1;

  PlanSpec() = default;
  PlanSpec(int pid, int mid, int jid)
      : plan_id(pid), map_collection_id(mid), join_collection_id(jid) {}

  std::string DebugString() const {
    std::stringstream ss;
    ss << "{ plan_id: " << plan_id;
    ss << ", map_collection_id: " << map_collection_id;
    ss << ", join_collection_id: " << join_collection_id;
    ss << ", num_iter: " << num_iter;
    ss << ", with_collection_id: " << with_collection_id;
    ss << "}";
    return ss.str();
  }

  /*
  friend SArrayBinStream& operator<<(xyz::SArrayBinStream& stream, const PlanSpec& p) {
    // TODO
  	return stream;
  }
  
  friend SArrayBinStream& operator>>(xyz::SArrayBinStream& stream, PlanSpec& p) {
    // TODO
  	return stream;
  }
  */
};

}  // namespace xyz
