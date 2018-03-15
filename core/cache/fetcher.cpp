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

std::shared_ptr<AbstractPartition> Fetcher::FetchPart(FetchMeta meta) {
  {
    // check partition cache
    std::unique_lock<std::mutex> lk(m_);
    if (IsVersionSatisfied(meta)) {
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
  auto p = partition_cache_[meta.collection_id][meta.partition_id];
  return partition_cache_[meta.collection_id][meta.partition_id];
}

void Fetcher::FinishPart(FetchMeta meta) {
  // for local part, do not forget to call FinishPart
  if (meta.local_mode && partition_manager_->Has(meta.collection_id, meta.partition_id)) {
    Message msg;
    msg.meta.sender = Qid();
    msg.meta.recver = GetControllerActorQid(GetNodeId(Qid()));
    CHECK_EQ(msg.meta.recver, 
      GetControllerActorQid(collection_map_->Lookup(meta.collection_id, meta.partition_id)));
    msg.meta.flag = Flag::kOthers;
    SArrayBinStream ctrl_bin, plan_bin, bin;
    ctrl_bin << ControllerFlag::kFinishFetch;
    plan_bin << meta.plan_id;
    bin << meta.partition_id << meta.upstream_part_id;
    msg.AddData(ctrl_bin.ToSArray());
    msg.AddData(plan_bin.ToSArray());
    msg.AddData(bin.ToSArray());
    sender_->Send(msg);
  }
}

void Fetcher::FetchPartRequest(Message msg) {
  SArrayBinStream bin;
  CHECK_EQ(msg.data.size(), 2);
  bin.FromSArray(msg.data[1]);
  FetchMeta meta;
  bin >> meta;
  auto& q = requesting_versions_[meta.collection_id][meta.partition_id];
  // check whether there are pending requests for this version
  // TODO: add the request logic
  
  // now I send fetch part directly
  SendFetchPart(meta);
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
  // LOG(INFO) << "local: " << meta.DebugString();
  auto p = partition_manager_->Get(meta.collection_id, meta.partition_id)->partition;

  std::unique_lock<std::mutex> lk(m_);
  // TODO: work with plan_controller for the version
  partition_versions_[meta.collection_id][meta.partition_id] = meta.version;
  partition_cache_[meta.collection_id][meta.partition_id] = p;
  cv_.notify_all();
}

void Fetcher::FetchPartReplyRemote(Message msg) {
  CHECK_EQ(msg.data.size(), 3);
  SArrayBinStream ctrl2_bin, bin;
  ctrl2_bin.FromSArray(msg.data[1]);
  bin.FromSArray(msg.data[2]);
  FetchMeta meta;
  ctrl2_bin >> meta;

  auto& func = function_store_->GetCreatePart(meta.collection_id);
  auto p = func();
  p->FromBin(bin);
  // TODO remove from requesting_versions_
  std::unique_lock<std::mutex> lk(m_);
  partition_versions_[meta.collection_id][meta.partition_id] = meta.version;
  partition_cache_[meta.collection_id][meta.partition_id] = p;
  cv_.notify_all();
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

  // 0. register rets
  recv_binstream_[upstream_part_id] = rets;
      
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
    FetchPartReplyRemote(msg);
  } else if (ctrl == FetcherFlag::kFetchPartReplyLocal){
    FetchPartReplyLocal(msg);
  } else {
    CHECK(false);
  }
}

}  // namespace xyz

