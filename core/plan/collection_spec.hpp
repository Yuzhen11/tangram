#pragma once

#include <sstream>

#include "base/sarray_binstream.hpp"

namespace xyz {

enum class CollectionSource : char {
  kDistribute,
  kLoad,
  kOthers
};

static const char* CollectionSourceName[] = {
  "kDistribute",
  "kLoad",
  "kOthers"
};

struct CollectionSpec {
  int collection_id;
  int num_partition;
  CollectionSource source;
  SArrayBinStream data;
  std::string load_url;

  std::string DebugString() const {
    std::stringstream ss;
    ss << "{ collection_id: " << collection_id;
    ss << ", num_partition: " << num_partition;
    ss << ", source: " << CollectionSourceName[static_cast<int>(source)];
    ss << ", data size in char: " << data.Size();
    ss << ", load_url: " << load_url;
    ss << "}";
    return ss.str();
  }

  friend SArrayBinStream& operator<<(xyz::SArrayBinStream& stream, const CollectionSpec& s) {
    stream << s.collection_id << s.num_partition << s.source << s.data << s.load_url;
  	return stream;
  }
  
  friend SArrayBinStream& operator>>(xyz::SArrayBinStream& stream, CollectionSpec& s) {
    stream >> s.collection_id >> s.num_partition >> s.source >> s.data >> s.load_url;
  	return stream;
  }
};

}  // namespace xyz

