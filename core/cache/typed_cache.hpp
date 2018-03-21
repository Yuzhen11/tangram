#pragma once

#include "core/index/abstract_key_to_part_mapper.hpp"
#include "core/cache/abstract_fetcher.hpp"
#include "core/scheduler/control.hpp"

namespace xyz {

template <typename ObjT>
class TypedCache {
 public:
  TypedCache(int plan_id, int partition_id, int version, 
          int collection_id, std::shared_ptr<AbstractFetcher> fetcher, 
          std::shared_ptr<AbstractKeyToPartMapper> mapper, 
          int staleness, bool local_mode)
      :plan_id_(plan_id), partition_id_(partition_id), version_(version),
       collection_id_(collection_id), fetcher_(fetcher), mapper_(mapper),
       staleness_(staleness), local_mode_(local_mode) {
    // LOG(INFO) << "Created TypedCache: cid: " << collection_id_;
  }
  int GetVersion() const {
    return version_;
  }

  std::vector<ObjT> Get(const std::vector<typename ObjT::KeyT>& keys) {
    // 1. sliced
    auto part_to_keys = Partition(keys);
    // 2. fetch
    std::vector<SArrayBinStream> rets;
    fetcher_->FetchObjs(plan_id_, partition_id_, collection_id_, part_to_keys, &rets);
    // 3. organize the result
    CHECK_EQ(rets.size(), part_to_keys.size());
    auto objs = Organzie(rets);
    CHECK_EQ(objs.size(), keys.size());
    return objs;
  }

  // set the local_mode to true to enable local fetch part
  std::shared_ptr<AbstractPartition> GetPartition(int partition_id) {
    FetchMeta meta;
    meta.plan_id = plan_id_;
    meta.upstream_part_id = -1;  // TODO: upstream_part_id may not be useful
    meta.collection_id = collection_id_;
    meta.partition_id = partition_id;
    meta.version = std::max(version_ - staleness_, 0);
    meta.local_mode = local_mode_;
    return fetcher_->FetchPart(meta);
  }

  // call FinishPart after accessing the part
  void ReleasePart(int partition_id) {
    FetchMeta meta;
    meta.plan_id = plan_id_;
    meta.upstream_part_id = -1;  // TODO: upstream_part_id may not be useful
    meta.collection_id = collection_id_;
    meta.partition_id = partition_id;
    meta.version = std::max(version_ - staleness_, 0);
    meta.local_mode = local_mode_;
    fetcher_->FinishPart(meta);
  }


  ObjT Get(typename ObjT::KeyT key) {
    CHECK(false);
  }

 private:
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

 private:
  std::shared_ptr<AbstractFetcher> fetcher_;
  std::shared_ptr<AbstractKeyToPartMapper> mapper_;
  int plan_id_;
  int collection_id_;
  int partition_id_;
  int version_;
  int staleness_ = 0;
  bool local_mode_ = true;
};

}  // namespace xyz


