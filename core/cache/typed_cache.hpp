#pragma once

#include "core/cache/abstract_cache.hpp"
#include "core/index/key_to_part_mappers.hpp"
#include "core/cache/abstract_partition_cache.hpp"

namespace xyz {

template <typename ObjT>
class TypedCache : public AbstractCache {
 public:
  TypedCache(std::shared_ptr<AbstractPartitionCache> partition_cache,
             std::shared_ptr<KeyToPartMappers> mappers)
    :partition_cache_(partition_cache), mappers_(mappers) {}

  ObjT Get(ObjT::KeyT key) {
  }

  ObjT Get(int collection_id, typename ObjT::KeyT key, int version) {
    CHECK(mappers_->Has(collection_id));
    auto mapper = mappers_->Get(collection_id);
    int partition_id = static_cast<TypedKeyToPartMapper<typename ObjT::KeyT>*>(mapper.get())->Get(key);

    auto part = partition_cache_->GetPartition(collection_id, partition_id, version);
    auto obj = static_cast<TypedPartition<ObjT>*>(part->partition.get())->Get(key);
    return obj;
  }

 private:
  std::shared_ptr<AbstractPartitionCache> partition_cache_;
  std::shared_ptr<KeyToPartMappers> mappers_;
};

}  // namespace xyz


