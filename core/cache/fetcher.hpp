#pragma once

#include <thread>
#include <queue>

#include "core/cache/abstract_fetcher.hpp"

#include "base/sarray_binstream.hpp"
#include "base/actor.hpp"
#include "core/index/abstract_part_to_node_mapper.hpp"
#include "core/partition/partition_manager.hpp"
#include "core/partition/abstract_partition.hpp"
#include "comm/abstract_sender.hpp"
#include "core/collection_map.hpp"
#include "core/queue_node_map.hpp"
#include "core/scheduler/control.hpp"
#include "core/plan/function_store.hpp"

namespace xyz {

class Fetcher : public Actor, public AbstractFetcher {
 public:
  Fetcher(int qid, std::shared_ptr<FunctionStore> function_store,
          std::shared_ptr<PartitionManager> partition_manager,
          std::shared_ptr<CollectionMap> collection_map, 
          std::shared_ptr<AbstractSender> sender):
    Actor(qid), function_store_(function_store),
    partition_manager_(partition_manager),
    collection_map_(collection_map),
    sender_(sender) {
    Start();
  }
  virtual ~Fetcher() {
    Stop();
  }

  virtual void FetchObjs(int plan_id, int upstream_part_id, int collection_id, 
        const std::map<int, SArrayBinStream>& part_to_keys,
        std::vector<SArrayBinStream>* const rets) override;

  virtual std::shared_ptr<AbstractPartition> FetchPart(FetchMeta meta) override;

  virtual void FinishPart(FetchMeta meta) override;


  void FetchPartRequest(Message msg);
  void SendFetchPart(FetchMeta meta);

  // for Process
  // void FetchObjsRequest(Message msg);
  void FetchObjsReply(Message msg);
  void FetchPartReplyRemote(Message msg);
  void FetchPartReplyLocal(Message msg);

  bool IsVersionSatisfied(const FetchMeta& meta);

  virtual void Process(Message msg) override;
 private:
  std::shared_ptr<CollectionMap> collection_map_;
  std::shared_ptr<AbstractSender> sender_;
  std::mutex m_;
  std::condition_variable cv_;
  // for fetch objs
  // upstream_part_id -> recv_count
  std::map<int, int> recv_finished_;
  // upstream_part_id -> (partition_id -> binstream)
  std::map<int, std::vector<SArrayBinStream>*> recv_binstream_;

  // for fetch partition
  // collection_id, part_id, version
  std::map<int, std::map<int, int>> partition_versions_;
  std::map<int, std::map<int, std::shared_ptr<AbstractPartition>>> partition_cache_;
  // collection_id, part_id, has_request
  std::map<int, std::map<int, std::deque<int>>> requesting_versions_;

  std::shared_ptr<FunctionStore> function_store_;
  std::shared_ptr<PartitionManager> partition_manager_;
};

}  // namespace xyz

