#include "core/worker/delayed_combiner.hpp"
#include "base/magic.hpp"

#include "glog/logging.h"

namespace xyz {

DelayedCombiner::DelayedCombiner(PlanController* plan_controller, int combine_timeout)
  : plan_controller_(plan_controller), combine_timeout_(combine_timeout) {
  store_.resize(plan_controller_->num_join_part_);
  if (combine_timeout_ > 0 && combine_timeout_ <= kMaxCombineTimeout) {
    detect_thread_ = std::thread([this]() {
      PeriodicCombine();
    });
  }
  // TODO: use a new executor or the existing one?
  executor_= std::make_shared<Executor>(kNumCombineThreads);
}

void DelayedCombiner::AddMapOutput(int upstream_part_id, int version, 
        std::shared_ptr<AbstractMapOutput> map_output) {
  std::lock_guard<std::mutex> lk(mu_);
  int buffer_size = map_output->GetBufferSize();
  CHECK_EQ(buffer_size, plan_controller_->num_join_part_);

  for (int part_id = 0; part_id < buffer_size; ++ part_id) {
    CHECK_LT(part_id, store_.size());
    AddStream(upstream_part_id, version, part_id, map_output->Get(part_id));
  }
}

void DelayedCombiner::AddStream(int upstream_part_id, int version, int part_id, 
        std::shared_ptr<AbstractMapOutputStream> stream) {
  if (combine_timeout_ < 0) {  // directly send without combine
    // executor_->Add([this, part_id, version, upstream_part_id, stream]() {
    //   PrepareMsgAndSend(part_id, version, {upstream_part_id}, stream);
    // });
    // use the same thread to send the message to ensure that
    // local output will be applied before next local map
    PrepareMsgAndSend(part_id, version, {upstream_part_id}, stream);
  } else if (combine_timeout_ == 0) {  // send with combine
    executor_->Add([this, part_id, version, upstream_part_id, stream]() {
      stream->Combine();
      PrepareMsgAndSend(part_id, version, {upstream_part_id}, stream);
    });
  } else {
    store_[part_id][version].push_back({upstream_part_id, stream});
    // TODO: the shuffle combine mode (combine all local map parts) may not work
    // with LB as it uses num_local_map_part_ variable which will be updated during
    // migration
    // Turn on PeriodicCombine to make sure all output will be flushed
    if (store_[part_id][version].size() == plan_controller_->num_local_map_part_) {
      Submit(part_id, version, std::move(store_[part_id][version]));
      store_[part_id].erase(version);
    }
  }
}

void DelayedCombiner::PeriodicCombine() {
  CHECK_GT(combine_timeout_, 0);
  while (true) {
    if (finished_.load() == true) {
      break;
    }  
    // TODO: may be hard to stop the thread if the combine_timeout_ is large
    std::this_thread::sleep_for(std::chrono::milliseconds(combine_timeout_));
    std::lock_guard<std::mutex> lk(mu_);
    for (int part_id = 0; part_id < store_.size(); ++ part_id) {
      for (auto& version_streams: store_[part_id]) {
        if (version_streams.second.size() > 0) {
          auto v = std::move(version_streams.second);
          Submit(part_id, version_streams.first, std::move(v));
        }
      }
      store_[part_id].clear();
    }
  }
}

void DelayedCombiner::Submit(int part_id, int version, std::vector<StreamPair> v) {
  executor_->Add([this, part_id, version, v]() {
    CombineSerializeSend(part_id, version, v);
  });
}

void DelayedCombiner::CombineSerializeSend(int part_id, int version, std::vector<StreamPair> v) {
  CHECK_GT(v.size(), 0);
  // 1. concatenate
  auto first_stream = v[0].second;
  for (int i = 1; i < v.size(); ++ i) {
    first_stream->Append(v[i].second);
    v[i].second->Clear();
  }
  // 2. combine
  first_stream->Combine();

  // 3. prepare message and send
  std::vector<int> upstream_part_ids;
  for (auto& p: v) {
    upstream_part_ids.push_back(p.first);
  }
  PrepareMsgAndSend(part_id, version, upstream_part_ids, first_stream);
}

void DelayedCombiner::PrepareMsgAndSend(int part_id, int version, 
        std::vector<int> upstream_part_ids, std::shared_ptr<AbstractMapOutputStream> stream) {
  // serialize before the lock.
  // for local mode, this serailization is unnecessary, 
  // but to support LB and for simplicity, we need to lock
  // the whole sending part. 
  // Another way is to use something like double-checked locking.
  auto bin = stream->Serialize();

  std::lock_guard<std::mutex> lk(plan_controller_->migrate_mu_);
  Message msg;
  msg.meta.sender = plan_controller_->controller_->Qid();
  CHECK(plan_controller_->controller_->engine_elem_.collection_map);
  msg.meta.recver = GetControllerActorQid(plan_controller_->controller_->engine_elem_.
          collection_map->Lookup(plan_controller_->join_collection_id_, part_id));
  msg.meta.flag = Flag::kOthers;

  PlanController::VersionedShuffleMeta meta;
  meta.plan_id = plan_controller_->plan_id_;
  meta.collection_id = plan_controller_->join_collection_id_;
  meta.upstream_part_id = -1;
  meta.ext_upstream_part_ids = upstream_part_ids;
  meta.part_id = part_id;
  meta.version = version;
  meta.local_mode = (msg.meta.recver == msg.meta.sender);

  SArrayBinStream ctrl_bin, plan_bin, ctrl2_bin;
  ctrl_bin << ControllerFlag::kReceiveJoin;
  plan_bin << plan_controller_->plan_id_;
  ctrl2_bin << meta;
  msg.AddData(ctrl_bin.ToSArray());
  msg.AddData(plan_bin.ToSArray());
  msg.AddData(ctrl2_bin.ToSArray());

  if (plan_controller_->local_map_mode_ && msg.meta.recver == msg.meta.sender) {
    auto k = std::make_tuple(part_id, upstream_part_ids, version);
    plan_controller_->stream_store_.Insert(k, stream);
    SArrayBinStream dummy_bin;
    msg.AddData(dummy_bin.ToSArray());
    plan_controller_->controller_->GetWorkQueue()->Push(msg);
  } else {
    msg.AddData(bin.ToSArray());
    plan_controller_->controller_->engine_elem_.intermediate_store->Add(msg);
  }
}

}  // namespace xyz

