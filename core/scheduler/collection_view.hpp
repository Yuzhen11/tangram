#pragma once

#include <sstream>

#include "core/index/simple_part_to_node_mapper.hpp"

namespace xyz {

struct CollectionView {
  int collection_id;
  int num_partition;
  SimplePartToNodeMapper mapper;

  std::string DebugString() const {
    std::stringstream ss;
    ss << "{ collection_id: " << collection_id;
    ss << ", num_partition: " << num_partition;
    ss << ", setup?: "
       << (mapper.GetNumParts() == num_partition ? "True" : "False");
    ss << ", mapper: " << mapper.DebugString();
    ss << "}";
    return ss.str();
  }

  friend SArrayBinStream &operator<<(xyz::SArrayBinStream &stream,
                                     const CollectionView &c) {
    stream << c.collection_id << c.num_partition << c.mapper;
    return stream;
  }

  friend SArrayBinStream &operator>>(xyz::SArrayBinStream &stream,
                                     CollectionView &c) {
    stream >> c.collection_id >> c.num_partition >> c.mapper;
    return stream;
  }
};

} // namespace xyz
