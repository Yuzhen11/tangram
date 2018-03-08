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
          std::map<int, GetterFuncT> funcs, std::shared_ptr<KeyToPartMappers> mappers,
          std::shared_ptr<CollectionMap> collection_map, 
          std::shared_ptr<AbstractSender> sender):
    Actor(qid), partition_manager_(partition_manager), func_(funcs), 
    mappers_(mappers), collection_map_(collection_map),
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

  template<typename ObjT>  
  std::vector<ObjT> Fetch(int app_thread_id, int collection_id, const std::vector<typename ObjT::KeyT>& keys) {
    // 1. send requests
    auto mapper = mappers_->Get(collection_id);
    std::map<int, std::vector<typename ObjT::KeyT>> part_to_keys;
    for (auto key : keys) {
      int partition_id = static_cast<TypedKeyToPartMapper<typename ObjT::KeyT>*> (mapper.get())->Get(key);
      part_to_keys[partition_id].push_back(key);
    }

    int recv_count = 0;
    for (auto const& pair : part_to_keys) {
      Message msg;
      msg.meta.sender = 
      msg.meta.recver = 
      msg.meta.flag = Flag::kOthers;
      Ctrl ctrl = Ctrl::kFetchObj;
      SArrayBinStream ctrl_bin, bin;
      ctrl_bin << ctrl;
      msg.AddData(ctrl_bin.ToSArray());
      
      bin << app_thread_id << collection_id << pair.first << pair.second;
      msg.AddData(bin.ToSArray());
      
      sender_->Send(msg);
      recv_count++;
    }
    
    // 2. wait requests
    {
      std::unique_lock<std::mutex> lk(m_);
      cv_.wait(lk, [this, app_thread_id, recv_count] {
        return recv_finished_[app_thread_id] == recv_count;
      });
    }

    // 3. organize the replies
    // TODO, now use a naive algorithm
    std::vector<ObjT> objs;
    {
      std::unique_lock<std::mutex> lk(m_);
      for (auto& kv: recv_binstream_[app_thread_id]) {
        auto bin = kv.second;
        while (bin.Size()) {
          ObjT obj;
          bin >> obj;
          objs.push_back(std::move(obj));
        }
      }
      recv_binstream_.erase(app_thread_id);
    }
    CHECK_EQ(objs.size(), keys.size());
    // assume keys are ordered
    std::sort(objs.begin(), objs.end(), [](const ObjT& o1, const ObjT& o2) {
      return o1.Key() < o2.Key();
    });

    /*
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
    */
  }


  void FetchLocalObj(Message msg) {
    CHECK_EQ(msg.data.size(), 2);
    SArrayBinStream bin;
    bin.FromSArray(msg.data[1]);
    int app_thread_id, collection_id, partition_id;  // TODO
    bin >> app_thread_id >> collection_id >> partition_id;
    CHECK(func_.find(collection_id) != func_.end());
    auto& func = func_[collection_id];
    CHECK(partition_manager_->Has(collection_id, partition_id));
    auto part = partition_manager_->Get(collection_id, partition_id);
    SArrayBinStream reply_bin;
    {
      boost::shared_lock<boost::shared_mutex> lk(part->mu);
      reply_bin = func(bin, part->partition);
    }
    Message reply_msg;
    SArrayBinStream ctrl_bin, ctrl2_bin;
    Ctrl ctrl = Ctrl::kFetchObjReply;
    ctrl_bin << ctrl;
    ctrl2_bin << app_thread_id << collection_id << partition_id; 
    reply_msg.AddData(ctrl_bin.ToSArray());
    reply_msg.AddData(ctrl2_bin.ToSArray());
    reply_msg.AddData(reply_bin.ToSArray());
    sender_->Send(std::move(reply_msg));
  }


  // void FetchReply(Message msg);

  void FetchObjReply(Message msg);

  // void setFunc(std::function <SArrayBinStream(SArrayBinStream, Fetcher*)> func) { func_ = func; }
  // void setMappers(std::shared_ptr<KeyToPartMappers> mappers) { mappers_ = mappers; }
  // std::shared_ptr<PartitionManager> getPartMng() { return partition_manager_; }
  virtual void Process(Message msg) override;

 private:
  std::map<int, GetterFuncT> func_;
  std::shared_ptr<KeyToPartMappers> mappers_;
  std::shared_ptr<PartitionManager> partition_manager_;
  std::shared_ptr<CollectionMap> collection_map_;
  // std::shared_ptr<AbstractPartitionCache> partition_cache_;
  std::shared_ptr<AbstractSender> sender_;
  std::mutex m_;
  std::condition_variable cv_;
  // app_thread_id -> recv_count
  std::map<int, int> recv_finished_;
  // app_thread_id -> (partition_id -> binstream)
  std::map<int, std::map<int, SArrayBinStream>> recv_binstream_;
};

}  // namespace xyz

