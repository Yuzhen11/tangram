#pragma once

#include <memory>

#include "base/sarray_binstream.hpp"

namespace xyz {

class AbstractPartition {
 public:
  virtual ~AbstractPartition() {}
  virtual void FromBin(SArrayBinStream& bin) = 0;
  virtual void ToBin(SArrayBinStream& bin) = 0;

  virtual size_t GetSize() const = 0;
  int id;
};

template <typename ObjT>
class Indexable {
 public:
  virtual ~Indexable() = default;
  virtual ObjT Get(typename ObjT::KeyT) {
    CHECK(false);
  }
  /*
   * The return pointer will be invalid when the storage size change.
   * You should assume the pointer is invalid once the partition changes.
   */
  virtual ObjT* FindOrCreate(typename ObjT::KeyT) {
    CHECK(false);
  }

  virtual void Sort() {
    CHECK(false);
  }
};

template <typename ObjT>
class TypedPartition : public AbstractPartition {
 public:
  // Add obj into partition
  void Add(ObjT obj) {
    TypedAdd(std::move(obj));
  }
  virtual void TypedAdd(ObjT obj) = 0;

  /*
   * Subclasses need to implement Iterator and implement CreateIterator() function
   * to support range-based for loop.
   */
  struct Iterator {
    virtual ObjT& Deref() = 0; 
    virtual ObjT* Ref() = 0; 
    virtual void SubAdvance() = 0;
    virtual bool SubUnequal(const std::unique_ptr<Iterator>& other) = 0;
    virtual ~Iterator() {}
  };
  struct IterWrapper {
    std::unique_ptr<Iterator> iter;
    ObjT& operator*() {
      return iter->Deref();
    }
    ObjT* operator->() {
      return iter->Ref();
    }
    IterWrapper& operator++() {
      iter->SubAdvance();
      return *this;
    }
    bool operator!=(const IterWrapper& iw) const {
      return iter->SubUnequal(iw.iter);
    }
    bool operator==(const IterWrapper& iw) const {
      return !iter->SubUnequal(iw.iter);
    }
  };
  IterWrapper begin() {
    return CreateIterator(true);
  }
  IterWrapper end() {
    return CreateIterator(false);
  }

  virtual IterWrapper CreateIterator(bool) {
    CHECK(false);
  }

};

}  // namespace
