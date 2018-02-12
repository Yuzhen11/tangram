#include "core/partition/fetcher.hpp"

namespace xyz {

void Fetcher::FetchRemote(int collection_id, int partition_id, int version) {
  // TODO: fill the meta.
  SArrayBinStream bin;
  bin << collection_id << partition_id << version;
  Message fetch_msg = bin.ToMsg();
  sender_->Send(fetch_msg);
}

void Fetcher::FetchLocal(Message msg) {
  Message reply_msg;
  // TODO: fill the meta.
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
    part->partition->ToBin(new_bin);
    reply_msg.AddData(new_bin.ToSArray());
  }
  sender_->Send(std::move(reply_msg));
}

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

void Fetcher::Process(Message msg) {
  CHECK_GE(msg.data.size(), 2);
  SArrayBinStream ctrl_bin, bin;
  ctrl_bin.FromSArray(msg.data[0]);
  bin.FromSArray(msg.data[1]);
  Ctrl ctrl;
  ctrl_bin >> ctrl;
  if (ctrl == Ctrl::kFetch) {
    FetchLocal(msg);
  } else if (ctrl == Ctrl::kFetchReply){
    FetchReply(msg);
  } else {
    CHECK(false);
  }
}

}  // namespace xyz

