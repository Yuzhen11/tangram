#pragma once

#include <memory>
#include <condition_variable>
#include <mutex>

#include "base/sarray_binstream.hpp"
#include "core/index/key_to_part_mappers.hpp"
#include "core/partition/partition_manager.hpp"
#include "core/cache/bin_to_part_mappers.hpp"
#include "core/partition/abstract_fetcher.hpp"
#include "core/cache/abstract_partition_cache.hpp"

#include "glog/logging.h"

namespace xyz {

class PartitionCache : public AbstractPartitionCache {
 public:
  PartitionCache(std::shared_ptr<PartitionManager> partition_manager,
          std::shared_ptr<KeyToPartMappers> mappers,
          std::shared_ptr<BinToPartMappers> bin_to_part_mappers,
          std::shared_ptr<AbstractFetcher> fetcher)
      :partition_manager_(partition_manager),
       mappers_(mappers),
       bin_to_part_mappers_(bin_to_part_mappers),
       fetcher_(fetcher) {
  }

  template<typename ObjT>
  ObjT Get(int collection_id, typename ObjT::KeyT key, int version) {
    CHECK(mappers_->Has(collection_id));
    auto mapper = mappers_->Get(collection_id);
    int partition_id = static_cast<TypedKeyToPartMapper<typename ObjT::KeyT>*>(mapper.get())->Get(key);
    // 1. From PartitionManager
    if (partition_manager_->Has(collection_id, partition_id, version)) {
      auto part = partition_manager_->Get(collection_id, partition_id);
      auto obj = static_cast<TypedPartition<ObjT>*>(part->partition.get())->Get(key);
      return obj;
    }
    // 2. From PartitionCache
    std::unique_lock<std::mutex> lk(mu_);
    if (partitions_[collection_id].find(partition_id) != partitions_[collection_id].end()
            && partitions_[collection_id][partition_id]->version >= version) {
      // exist, do nothing
    } else {
      // send request and wait
      fetcher_->FetchRemote(collection_id, partition_id, version);
      partitions_[collection_id][partition_id] = std::make_shared<VersionedPartition>();
      partitions_[collection_id][partition_id]->version = -1;
      cond_.wait(lk, [this, version, collection_id, partition_id] {
        auto part = partitions_[collection_id][partition_id];
        return version <= part->version;  // wait until version is satisfied.
      });
    }
    auto& part = partitions_[collection_id][partition_id];
    CHECK_LE(version, part->version);
    auto obj = static_cast<TypedPartition<ObjT>*>(part->partition.get())->Get(key);
    return obj;
  }
  virtual void UpdatePartition(int collection_id, int partition_id, int version, SArrayBinStream bin) override {
    auto part = std::make_shared<VersionedPartition>();
    part->version = version;
    part->partition = bin_to_part_mappers_->Call(collection_id, bin);
    std::lock_guard<std::mutex> lk(mu_);
    if (partitions_[collection_id].find(partition_id) != partitions_[collection_id].end()) {
      CHECK_LT(partitions_[collection_id][partition_id]->version, version);
    }
    partitions_[collection_id][partition_id] = std::move(part);
    cond_.notify_all();
  }
 private:
  std::shared_ptr<KeyToPartMappers> mappers_;
  std::shared_ptr<BinToPartMappers> bin_to_part_mappers_;
  std::shared_ptr<PartitionManager> partition_manager_;
  std::shared_ptr<AbstractFetcher> fetcher_;

  // The access of the partitions_ should be protected.
  // Finer-grained lock may be used later.
  std::map<int, std::map<int, std::shared_ptr<VersionedPartition>>> partitions_;
  std::mutex mu_;
  std::condition_variable cond_;
};

}  // namespace xyz

