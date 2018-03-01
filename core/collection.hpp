#pragma once

#include "core/scheduler/collection_view.hpp"

#include "core/index/abstract_key_to_part_mapper.hpp"

#include <memory>

namespace xyz {

template<typename T>
class Collection {
 public:
  int id;
  int num_partition;
  std::shared_ptr<AbstractKeyToPartMapper> mapper;

  Collection(int _id): Collection(_id, 1) {}
  Collection(int _id, int _num_part): 
    id(_id), num_partition(_num_part) {
  }

  CollectionView GetCollectionView() {
    CollectionView c;
    c.collection_id = id;
    c.num_partition = num_partition;
    return c;
  }
};

}  // namespace

