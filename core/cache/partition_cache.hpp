#pragma once

#include <memory>
#include <condition_variable>
#include <mutex>

#include "base/sarray_binstream.hpp"
#include "core/partition/partition_manager.hpp"
#include "core/cache/bin_to_part_mappers.hpp"
#include "core/partition/abstract_fetcher.hpp"
#include "core/cache/abstract_partition_cache.hpp"

#include "glog/logging.h"

namespace xyz {

class PartitionCache : public AbstractPartitionCache {
 public:
  PartitionCache(std::shared_ptr<PartitionManager> partition_manager,
          std::shared_ptr<BinToPartMappers> bin_to_part_mappers,
          std::shared_ptr<AbstractFetcher> fetcher)
      :partition_manager_(partition_manager),
       bin_to_part_mappers_(bin_to_part_mappers),
       fetcher_(fetcher) {
  }

  virtual std::shared_ptr<VersionedPartition> GetPartition(int collection_id, int partition_id, int version) override {
    std::shared_ptr<VersionedPartition> part;
    if (partition_manager_->Has(collection_id, partition_id, version)) {
      // 1. From PartitionManager
      part = partition_manager_->Get(collection_id, partition_id);
    } else {
      // 2. From PartitionCache
      part = GetPartitionFromCache(collection_id, partition_id, version);
    }
    CHECK_LE(version, part->version);
    return part;
  }

  std::shared_ptr<VersionedPartition> GetPartitionFromPartitionManager(int collection_id, int partition_id, int version) {
    return partition_manager_->Get(collection_id, partition_id);
  }

  std::shared_ptr<VersionedPartition> GetPartitionFromCache(int collection_id, int partition_id, int version) {
    std::unique_lock<std::mutex> lk(mu_);
    if (partitions_[collection_id].find(partition_id) == partitions_[collection_id].end()
            || partitions_[collection_id][partition_id]->version < version) {
      // if partition is not in partitions_ or it is too old
      // send request and wait
      partitions_[collection_id][partition_id] = std::make_shared<VersionedPartition>();
      partitions_[collection_id][partition_id]->version = -1;
      fetcher_->FetchRemote(collection_id, partition_id, version);
      cond_.wait(lk, [this, version, collection_id, partition_id] {
        auto part = partitions_[collection_id][partition_id];
        return version <= part->version;  // wait until version is satisfied.
      });
    }
    auto part = partitions_[collection_id][partition_id];
    return part;
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

