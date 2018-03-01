#pragma once

#include <sstream>

namespace xyz {

struct ShuffleMeta {
  int plan_id;
  int collection_id;
  int part_id;
  int upstream_part_id;

  std::string DebugString() const {
    std::stringstream ss;
    ss << "{";
    ss << " plan_id: " << plan_id;
    ss << ", collection_id: " << collection_id;
    ss << ", part_id: " << part_id;
    ss << ", upstream_part_id: " << upstream_part_id;
    ss << " }";
    return ss.str();
  }
};

}  // namespace xyz

