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
class IndexedSeqPartition : public SeqPartition<ObjT>, public Indexable<ObjT> {
 public:
  virtual void TypedAdd(ObjT obj) override {
    unsorted_[obj.Key()] = this->storage_.size();
    this->storage_.push_back(std::move(obj));
  }

  virtual ObjT Get(typename ObjT::KeyT key) override {
    ObjT* obj = Find(key);
    CHECK_NOTNULL(obj);
    return *obj;
  }

  virtual ObjT* FindOrCreate(typename ObjT::KeyT key) override {
    ObjT* obj = Find(key);
    if (obj) {
      return obj;
    }
    // If cannot find, add it.
    ObjT new_obj(key);  // Assume the constructor is low cost.
    TypedAdd(std::move(new_obj));
    return &this->storage_.back();
  }

  virtual ObjT* Find(typename ObjT::KeyT key) {
    ObjT obj(key);  // Assume the constructor is low cost.
    // 1. Find from sorted part.
    if (sorted_size_ != 0) {
      auto it_end = this->storage_.begin()+sorted_size_;
      auto it = std::lower_bound(this->storage_.begin(), it_end,
              obj, [](const ObjT& a, const ObjT& b) {
        return a.Key() < b.Key();
      });
      if (it != it_end) {
        return &(*it);
      }
    }
    // 2. Find from the unsorted part.
    if (!unsorted_.empty()) {
      auto it2 = unsorted_.find(key);
      if (it2 != unsorted_.end()) {
        return &(this->storage_[it2->second]);
      }
    }
    return nullptr;
  }

  virtual void FromBin(SArrayBinStream& bin) override {
    bin >> this->storage_;
    bin >> unsorted_;
    bin >> sorted_size_;
  }
  virtual void ToBin(SArrayBinStream& bin) override {
    bin << this->storage_;
    bin << unsorted_;
    bin << sorted_size_;
  }

  virtual void Sort() override {
    std::sort(this->storage_.begin(), this->storage_.end(), [](const ObjT& a, const ObjT& b) { return a.Key() < b.Key(); });
    unsorted_.clear();
    sorted_size_ = this->storage_.size();
  }

  size_t GetSortedSize() const {return this->storage_.size() - unsorted_.size(); }

  size_t GetUnsortedSize() const { return unsorted_.size(); }

 private:
  std::unordered_map<typename ObjT::KeyT, size_t> unsorted_;
  int sorted_size_ = 0;
};

}  // namespace

