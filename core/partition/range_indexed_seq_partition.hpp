#pragma once

#include "core/partition/seq_partition.hpp"
#include "base/third_party/range.h"

#include <vector>
#include <algorithm>

namespace xyz {

template <typename ObjT>
class RangeIndexedSeqPartition : public SeqPartition<ObjT>, public Indexable<ObjT> {
 public:
  RangeIndexedSeqPartition(const third_party::Range& range): range_(range) {
    this->storage_.resize(range.size());
  }
  
  virtual void TypedAdd(ObjT obj) override {
    this->storage_[obj.Key()] = std::move(obj);
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
    return &this->storage_[key];
  }

  virtual ObjT* Find(typename ObjT::KeyT key) {
    if ( key = std::clamp(key, range_.begin(), range_.end()) )
        return &this->storage[key];
    return nullptr;
  }

  virtual void FromBin(SArrayBinStream& bin) override {
    bin >> this->storage_;
    bin >> range_;
  }
  
  virtual void ToBin(SArrayBinStream& bin) override {
    bin << this->storage_;
    bin << range_;
  }

  virtual void Sort() override {
    LOG(INFO) << "Do Nothing";
  }

 private:
  third_party::Range range_;
};

}  // namespace

