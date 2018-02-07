#pragma once

#include <thread>

#include "base/threadsafe_queue.hpp"
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
class Fetcher : public AbstractFetcher {
 public:
  Fetcher(int qid, std::shared_ptr<PartitionManager> partition_manager,
          std::shared_ptr<AbstractPartitionCache> partition_cache,
          std::shared_ptr<AbstractSender> sender):
    queue_id_(qid), partition_manager_(partition_manager),
    partition_cache_(partition_cache), sender_(sender) {}

  ThreadsafeQueue<Message>* GetWorkQueue() { return &work_queue_; }

  virtual void FetchRemote(int collection_id, int partition_id, int version) override;

  /*
   * Fetch from local partition_manager
   * Invoked by Main().
   */
  void FetchLocal(Message msg);
  /*
   * Receive FetchReply from remote.
   * Invoked by Main().
   */
  void FetchReply(Message msg);
  void Main();
 private:
  std::shared_ptr<PartitionManager> partition_manager_;
  std::shared_ptr<AbstractPartitionCache> partition_cache_;
  std::shared_ptr<AbstractSender> sender_;

  uint32_t queue_id_;
  ThreadsafeQueue<Message> work_queue_;
  std::thread work_thread_;
};

}  // namespace xyz
