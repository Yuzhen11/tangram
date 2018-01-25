#pragma once

#include "core/index/abstract_key_to_part_mapper.hpp"

#include <memory>

namespace xyz {

template<typename T>
class Collection {
 public:
  int id;
  std::shared_ptr<AbstractKeyToPartMapper> mapper;
};

}  // namespace

