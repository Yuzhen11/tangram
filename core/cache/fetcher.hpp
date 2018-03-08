#pragma once

#include <thread>

#include "base/sarray_binstream.hpp"
#include "base/actor.hpp"
#include "core/index/key_to_part_mappers.hpp"
#include "core/index/abstract_part_to_node_mapper.hpp"
// #include "core/cache/abstract_partition_cache.hpp"
#include "core/partition/partition_manager.hpp"
#include "core/partition/abstract_partition.hpp"
#include "comm/abstract_sender.hpp"
#include "core/collection_map.hpp"
#include "core/queue_node_map.hpp"

namespace xyz {

/*
 * Fetch format: collection_id, partition_id, version, ...
 * FetchReply format: collection_id, partition_id, version, <partition>
 *
 * This is used to:
 * 1. Fetch remote partition (RemoteFetch)
 *     - called by partition_cache or other components
 * 2. Handle fetch request
 * 3. Handle fetch reply
 */
class Fetcher : public Actor {
 public:
  using GetterFuncT = std::function<
      SArrayBinStream(SArrayBinStream& bin, std::shared_ptr<AbstractPartition>)>;
  enum class Ctrl : char {
    kFetch, kFetchObj, kFetchReply, kFetchObjReply
  };
  Fetcher(int qid, std::shared_ptr<PartitionManager> partition_manager,
          const std::map<int, GetterFuncT>& funcs,
          std::shared_ptr<CollectionMap> collection_map, 
          std::shared_ptr<AbstractSender> sender):
    Actor(qid), partition_manager_(partition_manager), func_(funcs), 
    collection_map_(collection_map),
    sender_(sender) {
    Start();
  }
  virtual ~Fetcher() {
    Stop();
  }

  // virtual void FetchRemote(int collection_id, int partition_id, int version) override;

  /*
   * Fetch from local partition_manager
   * Invoked by Process().
   */
  // void FetchLocal(Message msg);
  /*
   * Receive FetchReply from remote.
   * Invoked by Process().
   */

  void Fetch(int app_thread_id, int collection_id, 
          const std::map<int, SArrayBinStream>& part_to_keys,
          std::vector<SArrayBinStream>* const rets) {

    // 0. register rets
    recv_binstream_[app_thread_id] = rets;
        
    // 1. send requests
    for (auto const& pair : part_to_keys) {
      Message msg;
      msg.meta.sender = GetFetcherQid(collection_map_->Lookup(collection_id, pair.first));
      msg.meta.recver = Qid();
      msg.meta.flag = Flag::kOthers;
      SArrayBinStream ctrl_bin, ctrl2_bin;
      ctrl_bin << Ctrl::kFetchObj;
      ctrl2_bin << app_thread_id << collection_id << pair.first;
      auto& bin = pair.second;
      msg.AddData(ctrl_bin.ToSArray());
      msg.AddData(ctrl2_bin.ToSArray());
      msg.AddData(bin.ToSArray());
      
      sender_->Send(msg);
    }
    
    // 2. wait requests
    int recv_count = part_to_keys.size();
    {
      std::unique_lock<std::mutex> lk(m_);
      cv_.wait(lk, [this, app_thread_id, recv_count] {
        return recv_finished_[app_thread_id] == recv_count;
      });
      recv_binstream_.erase(app_thread_id);
      recv_finished_.erase(app_thread_id);
    }
    return;
  }


  void FetchLocalObj(Message msg);


  // void FetchReply(Message msg);

  void FetchObjReply(Message msg);

  // void setFunc(std::function <SArrayBinStream(SArrayBinStream, Fetcher*)> func) { func_ = func; }
  // void setMappers(std::shared_ptr<KeyToPartMappers> mappers) { mappers_ = mappers; }
  // std::shared_ptr<PartitionManager> getPartMng() { return partition_manager_; }
  virtual void Process(Message msg) override;

 private:
  std::map<int, GetterFuncT> func_;
  std::shared_ptr<PartitionManager> partition_manager_;
  std::shared_ptr<CollectionMap> collection_map_;
  // std::shared_ptr<AbstractPartitionCache> partition_cache_;
  std::shared_ptr<AbstractSender> sender_;
  std::mutex m_;
  std::condition_variable cv_;
  // app_thread_id -> recv_count
  std::map<int, int> recv_finished_;
  // app_thread_id -> (partition_id -> binstream)
  std::map<int, std::vector<SArrayBinStream>*> recv_binstream_;
};

}  // namespace xyz

