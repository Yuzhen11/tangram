#pragma once

#include <sstream>

#include "base/sarray_binstream.hpp"

namespace xyz {

struct PlanSpec {
  int plan_id;
  int map_collection_id;
  int join_collection_id;

  int with_collection_id;

  int map_partition_num;
  int join_partition_num;
  int with_partition_num;

  PlanSpec() = default;

  std::string DebugString() const {
    std::stringstream ss;
    ss << "{ plan_id: " << plan_id;
    ss << ", map_collection_id: " << map_collection_id;
    ss << ", join_collection_id: " << join_collection_id;
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
