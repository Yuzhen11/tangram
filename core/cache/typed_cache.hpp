#pragma once

#include "core/cache/abstract_cache.hpp"
// #include "core/index/abstract_key_to_part_mapper.hpp"
// #include "core/cache/abstract_partition_cache.hpp"
// #include "core/partition/indexed_seq_partition.hpp"

#include "core/cache/fetcher.hpp"


namespace xyz {

template <typename ObjT>
class TypedCache : public AbstractCache {
 public:
  // TypedCache(std::shared_ptr<AbstractPartitionCache> partition_cache,
  //            std::shared_ptr<AbstractKeyToPartMapper> mapper,
  //            int collection_id, int version)
  //   :partition_cache_(partition_cache), mapper_(mapper), collection_id_(collection_id),
  //    version_(version){}
  //
  // ObjT Get(typename ObjT::KeyT key) {
  //   int partition_id = static_cast<TypedKeyToPartMapper<typename ObjT::KeyT>*>(mapper_.get())->Get(key);
  //
  //   auto part = partition_cache_->GetPartition(collection_id_, partition_id, version_);
  //   auto obj = static_cast<IndexedSeqPartition<ObjT>*>(part->partition.get())->Get(key);
  //   return obj;
  // }
 
  TypedCache(int collection_id, std::shared_ptr<Fetcher> fetcher, 
          std::shared_ptr<AbstractKeyToPartMapper> mapper)
      :collection_id_(collection_id), fetcher_(fetcher), mapper_(mapper) {
    // LOG(INFO) << "Created TypedCache: cid: " << collection_id_;
  }

  ObjT Get(typename ObjT::KeyT key) {
    CHECK(false);
  }


  std::map<int, SArrayBinStream> Partition(const std::vector<typename ObjT::KeyT>& keys) {
    auto* typed_mapper = static_cast<TypedKeyToPartMapper<typename ObjT::KeyT>*>(mapper_.get());
    std::map<int, SArrayBinStream> parts;
    for (auto key : keys) {
      int partition_id = typed_mapper->Get(key);
      parts[partition_id] << key;
    }
    return parts;
  }

  std::vector<ObjT> Organzie(std::vector<SArrayBinStream>& rets) {
    // TODO, now use a naive algorithm
    std::vector<ObjT> objs;
    for (auto& bin : rets) {
      while (bin.Size()) {
        ObjT obj;
        bin >> obj;
        objs.push_back(std::move(obj));
      }
    }
    // assume keys are ordered
    std::sort(objs.begin(), objs.end(), [](const ObjT& o1, const ObjT& o2) {
      return o1.Key() < o2.Key();
    });
    return objs;
  }

  std::vector<ObjT> Get(const std::vector<typename ObjT::KeyT>& keys) {
    int app_thread_id = 0;
    // 1. sliced
    auto part_to_keys = Partition(keys);
    // 2. fetch
    std::vector<SArrayBinStream> rets;
    fetcher_->FetchObjs(app_thread_id, collection_id_, part_to_keys, &rets);
    // 3. organize the result
    CHECK_EQ(rets.size(), part_to_keys.size());
    auto objs = Organzie(rets);
    CHECK_EQ(objs.size(), keys.size());
    return objs;
  }

 private:
  std::shared_ptr<Fetcher> fetcher_;
  std::shared_ptr<AbstractKeyToPartMapper> mapper_;
  int collection_id_;

  // std::shared_ptr<AbstractPartitionCache> partition_cache_;
  // int collection_id_;
  // // TODO: the system should decide the version.
  // int version_;
};

}  // namespace xyz


