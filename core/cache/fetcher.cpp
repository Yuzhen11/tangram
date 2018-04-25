#include "core/cache/fetcher.hpp"

namespace xyz {

// required to have lock first
bool Fetcher::IsVersionSatisfied(const FetchMeta& meta) {
  if (partition_versions_[meta.collection_id].find(meta.partition_id) != partition_versions_[meta.collection_id].end()
      && partition_versions_[meta.collection_id][meta.partition_id] >= meta.version) {
    return true;
  } else {
    return false;
  }
}

// required to have lock first
void Fetcher::TryAccessLocal(const FetchMeta& meta) {
  // if (meta.local_mode && partition_manager_->Has(meta.collection_id, meta.partition_id)) {
  //   local_access_count_[{meta.collection_id, meta.partition_id}] += 1;
  // }
  auto p = std::make_pair(meta.collection_id, meta.partition_id);
  if (local_access_count_.find(p) != local_access_count_.end()) {
    local_access_count_[p] += 1;
  }
}

std::shared_ptr<AbstractPartition> Fetcher::FetchPart(FetchMeta meta) {
  {
    // check partition cache
    std::unique_lock<std::mutex> lk(m_);
    if (IsVersionSatisfied(meta)) {
      TryAccessLocal(meta);
      // LOG(INFO) << "[FetchPart] accessing from process cache";
      return partition_cache_[meta.collection_id][meta.partition_id];
    }
  }
  Message msg;
  msg.meta.flag = Flag::kOthers;
  SArrayBinStream ctrl_bin, bin;
  ctrl_bin << FetcherFlag::kFetchPartRequest; 
  bin << meta;
  msg.AddData(ctrl_bin.ToSArray());
  msg.AddData(bin.ToSArray());
  GetWorkQueue()->Push(msg);
  {
    std::unique_lock<std::mutex> lk(m_);
    // wait until partition version > required version.
    cv_.wait(lk, [this, meta] {
      return IsVersionSatisfied(meta);
    });
  }
  std::unique_lock<std::mutex> lk(m_);
  TryAccessLocal(meta);
  auto p = partition_cache_[meta.collection_id][meta.partition_id];
  return partition_cache_[meta.collection_id][meta.partition_id];
}

void Fetcher::FinishPart(FetchMeta meta) {
    /*
  if (meta.local_mode && partition_manager_->Has(meta.collection_id, meta.partition_id)) {
    std::unique_lock<std::mutex> lk(m_);
    local_access_count_[{meta.collection_id, meta.partition_id}] -= 1;
    // when no one is accessing this part, SendFinishPart
    // TODO: the reference count of local part will drop to 0 even
    // when fetch_collection != join_collection (fetch_collection 
    // is immutable), and thus local part need to be refetched.
    // But it should be fast
    if (local_access_count_[{meta.collection_id, meta.partition_id}] == 0) {
      SendFinishPart(meta);
      int version = partition_versions_[meta.collection_id][meta.partition_id];
      partition_versions_[meta.collection_id].erase(meta.partition_id);
      partition_cache_[meta.collection_id].erase(meta.partition_id);
      TryFetchNextVersion(meta, version);
    }
  }
  */
  std::unique_lock<std::mutex> lk(m_);
  auto p = std::make_pair(meta.collection_id, meta.partition_id);
  if (local_access_count_.find(p) != local_access_count_.end()) {
    CHECK(local_access_count_[p] > 0);
    local_access_count_[p] -= 1;
    // when no one is accessing this part, SendFinishPart
    // TODO: the reference count of local part will drop to 0 even
    // when fetch_collection != join_collection (fetch_collection 
    // is immutable), and thus local part need to be refetched.
    // But it should be fast
    if (local_access_count_[p] == 0) {
      SendFinishPart(meta);
      int version = partition_versions_[meta.collection_id][meta.partition_id];
      partition_versions_[meta.collection_id].erase(meta.partition_id);
      partition_cache_[meta.collection_id].erase(meta.partition_id);
      TryFetchNextVersion(meta, version);
      local_access_count_.erase(p);
    }
  }
}

void Fetcher::SendFinishPart(const FetchMeta& meta) {
  // LOG(INFO) << "SendFinishPart: " << meta.DebugString();
  Message msg;
  msg.meta.sender = Qid();
  msg.meta.recver = GetControllerActorQid(GetNodeId(Qid()));
  // CHECK_EQ(msg.meta.recver, 
  //   GetControllerActorQid(collection_map_->Lookup(meta.collection_id, meta.partition_id)));
  msg.meta.flag = Flag::kOthers;
  SArrayBinStream ctrl_bin, plan_bin, bin;
  ctrl_bin << ControllerFlag::kFinishFetch;
  plan_bin << meta.plan_id;
  bin << meta.partition_id << meta.upstream_part_id;  // TODO: upstream_part_id may not be useful
  msg.AddData(ctrl_bin.ToSArray());
  msg.AddData(plan_bin.ToSArray());
  msg.AddData(bin.ToSArray());
  sender_->Send(msg);
}

void Fetcher::FetchPartRequest(Message msg) {
  SArrayBinStream bin;
  CHECK_EQ(msg.data.size(), 2);
  bin.FromSArray(msg.data[1]);
  FetchMeta meta;
  bin >> meta;

  std::lock_guard<std::mutex> lk(m_);
  if (IsVersionSatisfied(meta)) {
    return;
  }
  auto& q = requesting_versions_[meta.collection_id][meta.partition_id];
  // if q is empty,
  //    then push to q and fetch it!
  // else if the current pending requesting version is smaller,
  //    then push to q, may fetch it later if the response does not satisfy
  // else
  //    just waiting for the response
  if (q.empty()) {
    q.push_back(meta.version);
    SendFetchPart(meta);
  } else if (q.back() < meta.version) {
    q.push_back(meta.version);
  }
}

void Fetcher::SendFetchPart(FetchMeta meta) {
  // LOG(INFO) << "fetching: " << meta.DebugString();
  Message msg;
  msg.meta.sender = Qid();
  msg.meta.recver = GetControllerActorQid(collection_map_->Lookup(meta.collection_id, meta.partition_id));
  msg.meta.flag = Flag::kOthers;
  SArrayBinStream ctrl_bin, ctrl2_bin, dummy_bin;
  ctrl_bin << ControllerFlag::kFetchRequest; // send to controller
  ctrl2_bin << meta;
  msg.AddData(ctrl_bin.ToSArray());
  msg.AddData(ctrl2_bin.ToSArray());
  msg.AddData(dummy_bin.ToSArray());
  sender_->Send(msg);
}

void Fetcher::FetchPartReplyLocal(Message msg) {
  // access to local part granted
  CHECK_EQ(msg.data.size(), 2);
  SArrayBinStream ctrl2_bin, bin;
  ctrl2_bin.FromSArray(msg.data[1]);
  FetchMeta meta;
  ctrl2_bin >> meta;
  // LOG(INFO) << "fetcher receives local: " << meta.DebugString();
  // directly get the partition from partition_manager_
  auto p = partition_manager_->Get(meta.collection_id, meta.partition_id);

  std::unique_lock<std::mutex> lk(m_);
  partition_versions_[meta.collection_id][meta.partition_id] = meta.version;
  partition_cache_[meta.collection_id][meta.partition_id] = p;
  CHECK(local_access_count_.find({meta.collection_id, meta.partition_id}) == local_access_count_.end());
  local_access_count_[{meta.collection_id, meta.partition_id}] = 0;
  cv_.notify_all();
}

void Fetcher::FetchPartReplyRemote(Message msg) {
  CHECK_EQ(msg.data.size(), 3); 
  SArrayBinStream ctrl2_bin, bin;
  ctrl2_bin.FromSArray(msg.data[1]);
  bin.FromSArray(msg.data[2]);
  FetchMeta meta;
  ctrl2_bin >> meta;
  // LOG(INFO) << "fetcher receives remote: " << meta.DebugString();

  auto& func = function_store_->GetCreatePart(meta.collection_id);
  auto p = func();
  p->FromBin(bin);
  // LOG(INFO) << "debug, partition: " << meta.partition_id << ", size: " << p->GetSize();
  std::unique_lock<std::mutex> lk(m_);
  partition_versions_[meta.collection_id][meta.partition_id] = meta.version;
  partition_cache_[meta.collection_id][meta.partition_id] = p;
  cv_.notify_all();

  TryFetchNextVersion(meta, meta.version);
}

// lock required
void Fetcher::TryFetchNextVersion(const FetchMeta& meta, int version) {
  auto& q = requesting_versions_[meta.collection_id][meta.partition_id];
  q.erase(std::remove_if(q.begin(), q.end(), 
              [version](int v){ return v <= version; } )
          , q.end());
  // send new request for next version if any
  if (!q.empty()) {
    int next_version = q.front();
    CHECK_GT(next_version, meta.version);
    FetchMeta next_meta = meta;
    next_meta.version = next_version;
    SendFetchPart(next_meta);
  }
}

void Fetcher::FetchObjsReply(Message msg) {
  CHECK_EQ(msg.data.size(), 3);
  SArrayBinStream ctrl2_bin, bin;
  ctrl2_bin.FromSArray(msg.data[1]);
  bin.FromSArray(msg.data[2]);
  FetchMeta meta;
  ctrl2_bin >> meta;

  std::unique_lock<std::mutex> lk(m_);
  CHECK(recv_binstream_.find(meta.upstream_part_id) != recv_binstream_.end());
  CHECK_NOTNULL(recv_binstream_[meta.upstream_part_id]);
  recv_binstream_[meta.upstream_part_id]->push_back(bin);
  recv_finished_[meta.upstream_part_id]++;
  cv_.notify_all();
}

void Fetcher::FetchObjs(int plan_id, int upstream_part_id, int collection_id, 
        const std::map<int, SArrayBinStream>& part_to_keys,
        std::vector<SArrayBinStream>* const rets) {

  {
    std::unique_lock<std::mutex> lk(m_);
    // 0. register rets
    recv_binstream_[upstream_part_id] = rets;
  }
      
  // 1. send requests
  for (auto const& pair : part_to_keys) {
    Message msg;
    msg.meta.sender = Qid();
    msg.meta.recver = GetControllerActorQid(collection_map_->Lookup(collection_id, pair.first));// get controller qid
    msg.meta.flag = Flag::kOthers;
    SArrayBinStream ctrl_bin, ctrl2_bin;
    ctrl_bin << ControllerFlag::kFetchRequest;// send to controller
    FetchMeta fetch_meta;
    fetch_meta.plan_id = plan_id;
    fetch_meta.upstream_part_id = upstream_part_id;
    fetch_meta.collection_id = collection_id;
    fetch_meta.partition_id = pair.first;
    fetch_meta.version = -1;
    ctrl2_bin << fetch_meta;
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
    cv_.wait(lk, [this, upstream_part_id, recv_count] {
      return recv_finished_[upstream_part_id] == recv_count;
    });
    recv_binstream_.erase(upstream_part_id);
    recv_finished_.erase(upstream_part_id);
  }
  return;
}

void Fetcher::Process(Message msg) {
  CHECK_GE(msg.data.size(), 2);
  SArrayBinStream ctrl_bin;
  ctrl_bin.FromSArray(msg.data[0]);
  FetcherFlag ctrl;
  ctrl_bin >> ctrl;
  if (ctrl == FetcherFlag::kFetchObjsReply){
    FetchObjsReply(msg);
  } else if (ctrl == FetcherFlag::kFetchPartRequest){
    FetchPartRequest(msg);
  } else if (ctrl == FetcherFlag::kFetchPartReplyRemote){
    executor_->Add([this, msg]() {
      FetchPartReplyRemote(msg);
    });
  } else if (ctrl == FetcherFlag::kFetchPartReplyLocal){
    FetchPartReplyLocal(msg);
  } else {
    CHECK(false);
  }
}

}  // namespace xyz

