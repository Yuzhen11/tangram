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
  SArrayBinStream bin;
  bin.FromMsg(msg);
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
  for (int i = 0; i < msg.data.size(); ++ i) {
    SArrayBinStream bin;
    bin.FromSArray(msg.data[i]);
    bin >> collection_id >> partition_id >> version;
    partition_cache_->UpdatePartition(collection_id, partition_id, version, bin);
  }
}

void Fetcher::Main() {
  while (true) {
    Message msg;
    work_queue_.WaitAndPop(&msg);
    Control ctrl;
    SArrayBinStream bin;
    bin.FromMsg(msg);
    bin >> ctrl;
    if (ctrl.flag == Flag::kExit) {
      break;
    } else if (ctrl.flag == Flag::kFetch) {
      FetchLocal(msg);
    } else if (ctrl.flag == Flag::kFetchReply){
      FetchReply(msg);
    } else {
      CHECK(false) << "Unknown flag in msg: " << FlagName[static_cast<int>(ctrl.flag)];
    }
  }
}

}  // namespace xyz

