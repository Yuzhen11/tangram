#pragma once

#include <memory>

#include "core/index/abstract_key_to_part_mapper.hpp"

#include "glog/logging.h"

namespace xyz {

class KeyToPartMappers {
 public:
  KeyToPartMappers() = default;
  bool Has(int collection_id) {
    return mappers_.find(collection_id) != mappers_.end();
  }

  void Add(int collection_id, std::shared_ptr<AbstractKeyToPartMapper> mapper) {
    CHECK(mappers_.find(collection_id) == mappers_.end());
    mappers_.insert({collection_id, mapper});
  }
  std::shared_ptr<AbstractKeyToPartMapper> Get(int collection_id) {
    CHECK(mappers_.find(collection_id) != mappers_.end());
    return mappers_[collection_id];
  }
 private:
  std::map<int, std::shared_ptr<AbstractKeyToPartMapper>> mappers_;
};

}  // namespace xyz

