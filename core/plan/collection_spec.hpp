#pragma once

#include <sstream>

#include "base/sarray_binstream.hpp"

namespace xyz {

struct CollectionSpec {
  int collection_id;
  int num_partition;
  SArrayBinStream data;

  std::string DebugString() const {
    std::stringstream ss;
    ss << "{ collection_id: " << collection_id;
    ss << ", num_partition: " << num_partition;
    ss << ", data size in char: " << data.Size();
    ss << "}";
    return ss.str();
  }

  friend SArrayBinStream& operator<<(xyz::SArrayBinStream& stream, const CollectionSpec& s) {
    stream << s.collection_id << s.num_partition << s.data;
  	return stream;
  }
  
  friend SArrayBinStream& operator>>(xyz::SArrayBinStream& stream, CollectionSpec& s) {
    stream >> s.collection_id >> s.num_partition >> s.data;
  	return stream;
  }
};

}  // namespace xyz

