#pragma once

#include "core/partition/abstract_partition.hpp"

namespace xyz {

/*
 * Basic sequential partition implementation.
 * Support range-based for loop.
 */
template <typename ObjT>
class SeqPartition : public TypedPartition<ObjT> {
 public:
  virtual void TypedAdd(ObjT obj) override {
    storage_.push_back(std::move(obj));
  }

  virtual size_t GetSize() const override { return storage_.size(); }

  virtual void FromBin(SArrayBinStream& bin) override {
    bin >> storage_;
  }
  virtual void ToBin(SArrayBinStream& bin) override {
    bin << storage_;
  }

  /*
   * Implement the Iterator to support range-based for loop
   */
  struct Iterator : public TypedPartition<ObjT>::Iterator {
    Iterator(ObjT* ptr, size_t pos): ptr_(ptr), pos_(pos) {}
    virtual ObjT& Deref() {
      return ptr_[pos_];
    }
    virtual void SubAdvance()  {
      ++ pos_;
    }
    virtual bool SubUnequal(const std::unique_ptr<typename TypedPartition<ObjT>::Iterator>& other) {
      return pos_ != static_cast<Iterator*>(other.get())->pos_;
    }
    ObjT* ptr_;
    size_t pos_;
  };

  virtual typename TypedPartition<ObjT>::IterWrapper CreateIterator(bool is_begin) override {
    typename TypedPartition<ObjT>::IterWrapper iw;
    if (storage_.empty()) {
      iw.iter.reset(new typename SeqPartition<ObjT>::Iterator(nullptr, 0));
      return iw;
    }
    if (is_begin) {
      iw.iter.reset(new typename SeqPartition<ObjT>::Iterator(&storage_[0], 0));
    } else {
      iw.iter.reset(new typename SeqPartition<ObjT>::Iterator(&storage_[0], storage_.size()));
    }
    return iw;
  }

  std::vector<ObjT> GetStorage() {
    return storage_;
  }
 protected:
  std::vector<ObjT> storage_;
};

}  // namespace

