#pragma once

#include "core/map_output/map_output_stream.hpp"

#include "glog/logging.h"

#include <mutex>
#include <memory>
#include <map>
#include <vector>

namespace xyz {

class MapOutputStreamStore {
 public:
  MapOutputStreamStore() = default;
  ~MapOutputStreamStore() = default;

  void Insert(std::tuple<int,std::vector<int>,int> k, std::shared_ptr<AbstractMapOutputStream> v) {
    std::lock_guard<std::mutex> lk(mu_);
    CHECK(stream_store_.find(k) == stream_store_.end());
    stream_store_[k] = std::move(v);
  }

  void Remove(std::tuple<int,std::vector<int>,int> k) {
    std::lock_guard<std::mutex> lk(mu_);
    CHECK(stream_store_.find(k) != stream_store_.end());
    stream_store_.erase(k);
  }

  std::shared_ptr<AbstractMapOutputStream> Get(std::tuple<int,std::vector<int>,int> k) {
    std::lock_guard<std::mutex> lk(mu_);
    CHECK(stream_store_.find(k) != stream_store_.end());
    return stream_store_[k];
  }

 private:
  // part_id, {upstream_part_ids}, version
  std::map<std::tuple<int,std::vector<int>,int>, std::shared_ptr<AbstractMapOutputStream>> stream_store_;
  std::mutex mu_;
};

}  // namespace

