#pragma once

#include "core/index/abstract_part_to_node_mapper.hpp"

#include <functional>

namespace xyz {

class HashPartToNodeMapper : public AbstractPartToNodeMapper {
 public:
  HashPartToNodeMapper(int num_nodes) : AbstractPartToNodeMapper(num_nodes) {}
  virtual int Get(int part_id) {
    return std::hash<int>()(part_id) % this->num_nodes_;
  }
};

}  // namespace xyz

