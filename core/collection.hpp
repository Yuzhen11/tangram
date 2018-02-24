#pragma once

#include "core/scheduler/collection_view.hpp"

#include "core/index/abstract_key_to_part_mapper.hpp"

#include <memory>

namespace xyz {

template<typename T>
class Collection {
 public:
  int id;
  std::shared_ptr<AbstractKeyToPartMapper> mapper;
  int num_partition;

  CollectionView GetCollectionView() {
    CollectionView c;
    c.collection_id = id;
    c.num_partition = num_partition;
    return c;
  }
};

}  // namespace

