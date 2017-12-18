#pragma once

#include "core/abstract_partition.hpp"

namespace xyz {

template <typename Obj>
class Partition : public AbstractPartition {
 public:
  void Add(Obj obj) {
    storage_.push_back(std::move(obj));
  }
  typename std::vector<Obj>::iterator begin() {
    return storage_.begin();
  }
  typename std::vector<Obj>::iterator end() {
    return storage_.end();
  }
 private:
  std::vector<Obj> storage_;
};

}  // namespace

