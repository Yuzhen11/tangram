#pragma once

#include "core/cache/abstract_cache.hpp"
#include "core/index/abstract_key_to_part_mapper.hpp"
#include "core/cache/abstract_partition_cache.hpp"
#include "core/partition/indexed_seq_partition.hpp"

namespace xyz {

template <typename ObjT>
class TypedCache : public AbstractCache {
 public:
  TypedCache(std::shared_ptr<AbstractPartitionCache> partition_cache,
             std::shared_ptr<AbstractKeyToPartMapper> mapper,
             int collection_id, int version)
    :partition_cache_(partition_cache), mapper_(mapper), collection_id_(collection_id),
     version_(version){}

  ObjT Get(typename ObjT::KeyT key) {
    int partition_id = static_cast<TypedKeyToPartMapper<typename ObjT::KeyT>*>(mapper_.get())->Get(key);

    auto part = partition_cache_->GetPartition(collection_id_, partition_id, version_);
    auto obj = static_cast<IndexedSeqPartition<ObjT>*>(part->partition.get())->Get(key);
    return obj;
  }

 private:
  std::shared_ptr<AbstractPartitionCache> partition_cache_;
  std::shared_ptr<AbstractKeyToPartMapper> mapper_;
  int collection_id_;
  // TODO: the system should decide the version.
  int version_;
};

}  // namespace xyz


