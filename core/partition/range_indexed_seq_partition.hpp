#pragma once

#include "core/partition/seq_partition.hpp"
#include "base/third_party/range.h"

#include <vector>
#include <algorithm>

namespace xyz {

template <typename ObjT>
class RangeIndexedSeqPartition : public SeqPartition<ObjT>, public Indexable<ObjT> {
 public:
  RangeIndexedSeqPartition() = default;
  RangeIndexedSeqPartition(const third_party::Range& range): range_(range) {
    this->storage_.resize(range.size());
  }
  
  virtual void TypedAdd(ObjT obj) override {
    CHECK(false);
    // this->storage_[obj.Key()] = std::move(obj);
  }

  virtual ObjT Get(typename ObjT::KeyT key) override {
    ObjT* obj = Find(key);
    CHECK_NOTNULL(obj);
    return *obj;
  }

  virtual ObjT* FindOrCreate(typename ObjT::KeyT key) override {
    // LOG(INFO) << "FindOrCreate: " << key;
    return Find(key);
    /*
    ObjT* obj = Find(key);
    if (obj) {
      return obj;
    }
    // If cannot find, add it.
    ObjT new_obj(key);  // Assume the constructor is low cost.
    TypedAdd(std::move(new_obj));
    return &this->storage_[key];
    */
  }

  virtual ObjT* Find(typename ObjT::KeyT key) {
    CHECK_GE(key, range_.begin());
    CHECK_LT(key, range_.end());
    return &this->storage_[key - range_.begin()];
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

