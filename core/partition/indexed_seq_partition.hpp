#pragma once

#include "core/partition/seq_partition.hpp"

#include <vector>
#include <unordered_map>
#include <algorithm>

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
    is_sorted_ = false;
  }

  virtual ObjT Get(typename ObjT::KeyT key) override {
    CHECK(is_sorted_) << "Now I assume it is sorted";
    ObjT obj(key);
    auto it = std::lower_bound(this->storage_.begin(), this->storage_.end(), 
            obj, [](const ObjT& a, const ObjT& b) {
      return a.Key() < b.Key();
    });
    CHECK(it != this->storage_.end());
    return *it;
  }

  virtual void FromBin(SArrayBinStream& bin) override {
    bin >> this->storage_;
    // TODO do not consider unsorted
  }
  virtual void ToBin(SArrayBinStream& bin) override {
    bin << this->storage_;
    // TODO do not consider unsorted
  }

  virtual void Sort() override {
    std::sort(this->storage_.begin(), this->storage_.end(), [](const ObjT& a, const ObjT& b) { return a.Key() < b.Key(); });
    unsorted_.clear();
    is_sorted_ = true;
  }

  size_t GetSortedSize() const {return this->storage_.size() - unsorted_.size(); }

  size_t GetUnsortedSize() const { return unsorted_.size(); }

 private:
  std::unordered_map<typename ObjT::KeyT, size_t> unsorted_;
  bool is_sorted_ = true;
};

}  // namespace

