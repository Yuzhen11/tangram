#pragma once

#include "core/index/simple_part_to_node_mapper.hpp"

namespace xyz {

struct CollectionView {
  int collection_id;
  int num_partition;
  SimplePartToNodeMapper mapper;

  friend SArrayBinStream& operator<<(xyz::SArrayBinStream& stream, const CollectionView& c) {
    stream << c.collection_id << c.num_partition << c.mapper;
  	return stream;
  }
  
  friend SArrayBinStream& operator>>(xyz::SArrayBinStream& stream, CollectionView& c) {
    stream >> c.collection_id >> c.num_partition >> c.mapper;
  	return stream;
  }
};

}  // namespace xyz

