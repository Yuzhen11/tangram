#pragma once

#include "core/scheduler/collection_view.hpp"
#include "core/abstract_collection_map.hpp"

#include "glog/logging.h"

namespace xyz {

class CollectionMap : public AbstractCollectionMap {
 public: 
  void Init(std::unordered_map<int, CollectionView> collection_map){
    std::lock_guard<std::mutex> lk(mu_);
    collection_map_ = collection_map;
  }
  void Insert(CollectionView cv) {
    std::lock_guard<std::mutex> lk(mu_);
    collection_map_[cv.collection_id] = cv;
  }
  int GetNumParts(int cid) {
    std::lock_guard<std::mutex> lk(mu_);
    CHECK(collection_map_.find(cid) != collection_map_.end());
    return collection_map_[cid].num_partition;
  }
  virtual int Lookup(int collection_id, int part_id) override {
    std::lock_guard<std::mutex> lk(mu_);
    CHECK(collection_map_.find(collection_id) != collection_map_.end());
    auto c = collection_map_[collection_id];
    int ret = c.mapper.Get(part_id);
    return ret;
  }
 private:
  std::unordered_map<int, CollectionView> collection_map_;
  std::mutex mu_;
};

}  // namespace xyz

