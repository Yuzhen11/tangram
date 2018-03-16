#pragma once

#include "core/plan/abstract_function_store.hpp"
#include "core/partition/abstract_partition.hpp"

namespace xyz {

template<typename T, typename MsgT>
AbstractFunctionStore::JoinFuncT GetJoinPartFunc(std::function<void(T*, const MsgT&)> join) {
  return [join] (std::shared_ptr<AbstractPartition> partition, SArrayBinStream bin) {
    auto* p = dynamic_cast<Indexable<T>*>(partition.get());
    CHECK_NOTNULL(p);
    typename T::KeyT key;
    MsgT msg;
    while (bin.Size()) {
      bin >> key >> msg;
      auto* obj = p->FindOrCreate(key);
      join(obj, msg);
    }
  };
}

template<typename T, typename MsgT>
AbstractFunctionStore::JoinFunc2T GetJoinPartFunc2(std::function<void(T*, const MsgT&)> join) {
  return [join] (std::shared_ptr<AbstractPartition> partition, std::shared_ptr<AbstractMapOutputStream> stream) {
    auto* p = dynamic_cast<Indexable<T>*>(partition.get());
    auto* s = static_cast<MapOutputStream<typename T::KeyT, MsgT>*>(stream.get());
    CHECK_NOTNULL(p);
    CHECK_NOTNULL(s);
    const auto& buffer = s->GetBuffer();
    for (auto& kv : buffer) {
      auto* obj = p->FindOrCreate(kv.first);
      join(obj, kv.second);
    }
  };
}

}  // namespace xyz

