#pragma once

#include <thread>

#include "base/sarray_binstream.hpp"
#include "base/actor.hpp"
#include "core/index/key_to_part_mappers.hpp"
#include "core/index/abstract_part_to_node_mapper.hpp"
#include "core/cache/abstract_partition_cache.hpp"
#include "core/partition/partition_manager.hpp"
#include "core/partition/abstract_fetcher.hpp"
#include "comm/abstract_sender.hpp"

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
class Fetcher : public AbstractFetcher, public Actor {
 public:
  enum class Ctrl : char {
    kFetch, kFetchObj, kFetchReply, kFetchObjReply
  };
  Fetcher(int qid, std::shared_ptr<PartitionManager> partition_manager,
          std::shared_ptr<AbstractPartitionCache> partition_cache,
          std::shared_ptr<AbstractSender> sender):
    Actor(qid), partition_manager_(partition_manager),
    partition_cache_(partition_cache), sender_(sender) {
    Start();
  }
  virtual ~Fetcher() {
    Stop();
  }

  virtual void FetchRemote(int collection_id, int partition_id, int version) override;

  /*
   * Fetch from local partition_manager
   * Invoked by Process().
   */
  void FetchLocal(Message msg);
  /*
   * Receive FetchReply from remote.
   * Invoked by Process().
   */

  template<typename ObjT>  
  std::vector<ObjT> FetchRemoteObj(int app_thread_id, int collection_id, std::vector<typename ObjT::KeyT> keys, int version) {
    SArrayBinStream bin;
    int recv_count = 0;
    auto mapper = mappers_->Get(collection_id);
    std::map<int, std::vector<typename ObjT::KeyT>> part_to_keys;
    for (auto key : keys) {
      int partition_id = static_cast<TypedKeyToPartMapper<typename ObjT::KeyT>*> (mapper.get())->Get(key);
      //if(part_to_keys[partition_id].empty()) {
      //   part_to_keys[partition_id] = std::vector<typename ObjT::KeyT> ();
      //}
      part_to_keys[partition_id].push_back(key);
    }

    for (auto const& pair : part_to_keys) {
      Message fetch_msg;
      Ctrl ctrl = Ctrl::kFetchObj;
      SArrayBinStream ctrl_bin, bin1;
      ctrl_bin << ctrl;
      fetch_msg.AddData(ctrl_bin.ToSArray());
      
      bin1 << app_thread_id << collection_id << pair.first << pair.second << version;
      fetch_msg.AddData(bin1.ToSArray());
      
      sender_->Send(fetch_msg);
      recv_count++;
    }
    
    std::unique_lock<std::mutex> lk(m_);
    cv_.wait(lk, [this, app_thread_id, recv_count] {
            return recv_finished_[app_thread_id] == recv_count;
            });
    
    std::vector<ObjT> ret_objs;
    std::map<int, std::vector<ObjT>> part_objs_map;
    std::map<int, int> index_map;
    for (auto& pair : recv_binstream_[app_thread_id]) {
        std::vector<ObjT> objs(part_to_keys[pair.first].size());
        pair.second >> objs;
        part_objs_map[pair.first] = objs;
    }

    for (auto key : keys) {
      int index;
      int partition_id = static_cast<TypedKeyToPartMapper<typename ObjT::KeyT>*> (mapper.get())->Get(key);
      if (index_map.find(partition_id) == index_map.end()) {
        index_map[partition_id] = 0;
        index = 0;
      } else {
        index = ++index_map[partition_id];
      }
      ObjT &k = part_objs_map[partition_id][index];
      ret_objs.push_back(k);
    }
    return ret_objs;
  }


  void FetchLocalObj(Message msg) {
    Message reply_msg;
    Ctrl ctrl = Ctrl::kFetchObjReply;
    SArrayBinStream ctrl_bin;
    ctrl_bin << ctrl;
    reply_msg.AddData(ctrl_bin.ToSArray());
    CHECK_EQ(msg.data.size(), 2);
    SArrayBinStream bin,new_bin;
    new_bin = func_(bin, this);
    // while (bin.Size()) {
    //   std::vector<typename ObjT::KeyT> keys;
    //   bin >> app_thread_id >> collection_id >> partition_id >> keys >> version;
    //   // TODO: what if the required version is not there ?
    //   CHECK(partition_manager_->Has(collection_id, partition_id, version));
    //   auto part = partition_manager_->Get(collection_id, partition_id, version);
    //   auto* indexed_part = dynamic_cast<Indexable<ObjT>*>(part->partition.get());
    //   std::vector<ObjT> objs;
    //   SArrayBinStream new_bin;  // collection_id, partition_id, version, <partition>
    //   for (auto key : keys) {
    //     objs.push_back(indexed_part->Get(key));
    //   }
    //   new_bin << app_thread_id << collection_id << partition_id << objs;
    //   reply_msg.AddData(new_bin.ToSArray());
    // }
    reply_msg.AddData(new_bin.ToSArray());
    sender_->Send(std::move(reply_msg));
  }


  void FetchReply(Message msg);

  void FetchObjReply(Message msg);

  void setFunc(std::function <SArrayBinStream(SArrayBinStream, Fetcher*)> func) { func_ = func; }
  void setMappers(std::shared_ptr<KeyToPartMappers> mappers) { mappers_ = mappers; }
  std::shared_ptr<PartitionManager> getPartMng() { return partition_manager_; }

  virtual void Process(Message msg) override;

 private:
  std::shared_ptr<KeyToPartMappers> mappers_;
  std::shared_ptr<PartitionManager> partition_manager_;
  std::shared_ptr<AbstractPartitionCache> partition_cache_;
  std::shared_ptr<AbstractSender> sender_;
  std::mutex m_;
  std::function <SArrayBinStream(SArrayBinStream, Fetcher*)> func_;
  std::condition_variable cv_;
  // app_thread_id -> recv_count
  std::map<int, int> recv_finished_;
  // app_thread_id -> (partition_id -> binstream)
  std::map<int, std::map<int, SArrayBinStream>> recv_binstream_;
};

}  // namespace xyz

