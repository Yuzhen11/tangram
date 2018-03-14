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
#include "core/scheduler/control.hpp"

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
  Fetcher(int qid, std::shared_ptr<PartitionManager> partition_manager,
          std::shared_ptr<CollectionMap> collection_map, 
          std::shared_ptr<AbstractSender> sender):
    Actor(qid), partition_manager_(partition_manager),
    collection_map_(collection_map),
    sender_(sender) {
    Start();
  }
  virtual ~Fetcher() {
    Stop();
  }

  // public api:
  void FetchObjs(int plan_id, int app_thread_id, int collection_id, 
        const std::map<int, SArrayBinStream>& part_to_keys,
        std::vector<SArrayBinStream>* const rets);

  void FetchPart(int collection_id, int partition_id);

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


  // for Process
  // void FetchObjsRequest(Message msg);
  void FetchObjsReply(Message msg);
  void FetchPartRequest(Message msg);
  void FetchPartReply(Message msg);


  virtual void Process(Message msg) override;
 private:
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

