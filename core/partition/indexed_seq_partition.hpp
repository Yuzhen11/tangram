#pragma once

#include "core/partition/seq_partition.hpp"

#include <vector>
#include <unordered_map>

namespace xyz {

/*
 * Largely borrowed from objlist.hpp from Husky project.
 * Use an unordered_map to track the unsorted part.
 * Requires ObjT to be in the form { ObjT::KeyT, ObjT::ValT }.
 * ObjT should have the function: Key().
 */
template <typename ObjT>
class IndexedSeqPartition : public SeqPartition<ObjT> {
 public:
  virtual void TypedAdd(ObjT obj) override {
    unsorted_[obj.Key()] = this->storage_.size();
    this->storage_.push_back(std::move(obj));
  }

  virtual void Sort() override {
    std::sort(this->storage_.begin(), this->storage_.end(), [](const ObjT& a, const ObjT& b) { return a.Key() < b.Key(); });
    unsorted_.clear();
  }

  size_t GetSortedSize() const {return this->storage_.size() - unsorted_.size(); }

  size_t GetUnsortedSize() const { return unsorted_.size(); }

 private:
  std::unordered_map<typename ObjT::KeyT, size_t> unsorted_;
};

}  // namespace

