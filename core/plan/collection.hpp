#pragma once

#include <vector>
#include <memory>
#include <sstream>

#include "core/plan/abstract_function_store.hpp"
#include "core/plan/collection_spec.hpp"
#include "core/scheduler/collection_view.hpp"

#include "core/index/abstract_key_to_part_mapper.hpp"
#include "core/partition/indexed_seq_partition.hpp"


namespace xyz {

struct CollectionBase {
  virtual ~CollectionBase() = default;
  virtual void Register(std::shared_ptr<AbstractFunctionStore> function_store) = 0;
};

template<typename T>
AbstractFunctionStore::GetterFuncT GetGetterFunc() {
  return [](SArrayBinStream& bin, std::shared_ptr<AbstractPartition> partition) {
    auto* p = dynamic_cast<Indexable<T>*>(partition.get());
    CHECK_NOTNULL(p);
    typename T::KeyT key;
    SArrayBinStream reply_bin;
    while (bin.Size()) {
      bin >> key;
      reply_bin << *p->FindOrCreate(key);
    }
    return reply_bin;
  };
}

template<typename T, typename PartitionT = IndexedSeqPartition<T>>
class Collection : public CollectionBase {
 public:
  using ObjT = T;
  Collection(int id): Collection(id, 1) {}
  Collection(int id, int num_part): 
    id_(id), num_partition_(num_part) {
  }

  int Id() const {
    return id_;
  }
  
  void SetMapper(std::shared_ptr<AbstractKeyToPartMapper> mapper) {
    mapper_ = mapper;
  }

  std::shared_ptr<AbstractKeyToPartMapper> GetMapper() {
    return mapper_;
  }

  template<typename Q = PartitionT>
  typename std::enable_if<!std::is_same<Q, IndexedSeqPartition<T>>::value>::type
  RegisterHelper(
          std::shared_ptr<AbstractFunctionStore> function_store) {
    // do nothing
  }

  template<typename Q = PartitionT>
  typename std::enable_if<std::is_same<Q, IndexedSeqPartition<T>>::value>::type 
  RegisterHelper(
          std::shared_ptr<AbstractFunctionStore> function_store) {
    function_store->AddGetter(id_, GetGetterFunc<T>());
  }

  virtual void Register(std::shared_ptr<AbstractFunctionStore> function_store) override {
     RegisterHelper(function_store);
  }

 private:
  int id_;
  int num_partition_;
  std::shared_ptr<AbstractKeyToPartMapper> mapper_;
};

}  // namespace

