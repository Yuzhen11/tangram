#include "core/cache/fetcher.hpp"

namespace xyz {


void Fetcher::FetchPart(int collection_id, int partition_id) {
}
void Fetcher::FetchPartRequest(Message msg) {
}
void Fetcher::FetchPartReply(Message msg) {
}

/*
void Fetcher::FetchRemote(int collection_id, int partition_id, int version) {
  Message fetch_msg;
  Ctrl ctrl = Ctrl::kFetch;
  SArrayBinStream ctrl_bin;
  ctrl_bin << ctrl;
  fetch_msg.AddData(ctrl_bin.ToSArray());
  
  SArrayBinStream bin;
  bin << collection_id << partition_id << version;
  fetch_msg.AddData(bin.ToSArray());

  sender_->Send(fetch_msg);
}

  

void Fetcher::FetchLocal(Message msg) {
  Message reply_msg;
  Ctrl ctrl = Ctrl::kFetchReply;
  SArrayBinStream ctrl_bin;
  ctrl_bin << ctrl;
  reply_msg.AddData(ctrl_bin.ToSArray());
  CHECK_EQ(msg.data.size(), 2);
  SArrayBinStream bin;
  bin.FromSArray(msg.data[1]);
  int collection_id;
  int partition_id;
  int version;
  while (bin.Size()) {
    bin >> collection_id >> partition_id >> version;
    // TODO: what if the required version is not there ?
    CHECK(partition_manager_->Has(collection_id, partition_id, version));
    auto part = partition_manager_->Get(collection_id, partition_id, version);
    SArrayBinStream new_bin;  // collection_id, partition_id, version, <partition>
    new_bin << collection_id << partition_id << part->version;
    {
        boost::shared_lock<boost::shared_mutex> lk(part->mu);
        part->partition->ToBin(new_bin);
    }
    reply_msg.AddData(new_bin.ToSArray());
  }
  sender_->Send(std::move(reply_msg));
}
*/


/*
void Fetcher::FetchReply(Message msg) {
  int collection_id;
  int partition_id;
  int version;
   
  for (int i = 1; i < msg.data.size(); ++ i) {  // start from 1
    SArrayBinStream bin;
    bin.FromSArray(msg.data[i]);
    bin >> collection_id >> partition_id >> version;
    partition_cache_->UpdatePartition(collection_id, partition_id, version, bin);
  }           
}
*/

void Fetcher::FetchObjsReply(Message msg) {
  int app_thread_id;
  int collection_id;
  int partition_id;
   
  CHECK_EQ(msg.data.size(), 3);
  SArrayBinStream ctrl2_bin, bin;
  ctrl2_bin.FromSArray(msg.data[1]);
  bin.FromSArray(msg.data[2]);
  ctrl2_bin >> app_thread_id >> collection_id >> partition_id;

  std::unique_lock<std::mutex> lk(m_);
  CHECK(recv_binstream_.find(app_thread_id) != recv_binstream_.end());
  CHECK_NOTNULL(recv_binstream_[app_thread_id]);
  recv_binstream_[app_thread_id]->push_back(bin);
  recv_finished_[app_thread_id]++;
  cv_.notify_all();
}

void Fetcher::FetchObjs(int plan_id, int app_thread_id, int collection_id, 
        const std::map<int, SArrayBinStream>& part_to_keys,
        std::vector<SArrayBinStream>* const rets) {

  // 0. register rets
  recv_binstream_[app_thread_id] = rets;
      
  // 1. send requests
  for (auto const& pair : part_to_keys) {
    Message msg;
    msg.meta.sender = Qid();
    msg.meta.recver = GetControllerActorQid(collection_map_->Lookup(collection_id, pair.first));// get controller qid
    msg.meta.flag = Flag::kOthers;
    SArrayBinStream ctrl_bin, ctrl2_bin;
    ctrl_bin << ControllerFlag::kFetchObjsRequest;// send to controller
    ctrl2_bin << plan_id << app_thread_id << collection_id << pair.first;
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

void Fetcher::Process(Message msg) {
  CHECK_GE(msg.data.size(), 2);
  SArrayBinStream ctrl_bin, bin;
  ctrl_bin.FromSArray(msg.data[0]);
  bin.FromSArray(msg.data[1]);
  FetcherFlag ctrl;
  ctrl_bin >> ctrl;
  if (ctrl == FetcherFlag::kFetch) {
    // FetchLocal(msg);
  } else if (ctrl == FetcherFlag::kFetchReply){
    // FetchReply(msg);
  } else if (ctrl == FetcherFlag::kFetchObjsRequest){
    // FetchObjsRequest(msg);
    CHECK(false);
  } else if (ctrl == FetcherFlag::kFetchObjsReply){
    FetchObjsReply(msg);
  } else {
    CHECK(false);
  }
}

}  // namespace xyz

