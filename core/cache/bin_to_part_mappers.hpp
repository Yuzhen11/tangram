#pragma once

#include <map>
#include <functional>

#include "base/sarray_binstream.hpp"
#include "core/partition/abstract_partition.hpp"

namespace xyz {

class BinToPartMappers {
 public: 
  using BinToPartFuncT = std::function<std::shared_ptr<AbstractPartition>(SArrayBinStream bin)>;
  void Add(int id, BinToPartFuncT func) {
    CHECK(mappers_.find(id) == mappers_.end());
    mappers_.insert({id, func});
  }
  std::shared_ptr<AbstractPartition> Call(int id, SArrayBinStream bin) {
    CHECK(mappers_.find(id) != mappers_.end());
    return mappers_[id](bin);
  }
 private:
  std::map<int, BinToPartFuncT> mappers_;
};

}  // namespace xyz

