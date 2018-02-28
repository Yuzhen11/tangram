#include "io/loader.hpp"

#include "core/partition/seq_partition.hpp"

#include "core/scheduler/control.hpp"

namespace xyz {

void Loader::Load(AssignedBlock block) {
  num_added_ += 1;
  executor_->Add([this, block]() {
    // 1. read
    auto strs = reader_->Read(namenode_, port_, block.url, block.offset);
    auto part = std::make_shared<SeqPartition<std::string>>();
    for (auto& s : strs) {
      part->Add(std::move(s));
    }  // TODO: make it more efficient
    partition_manager_->Insert(block.collection_id, block.id, std::move(part));

    // 2. reply
    SArrayBinStream ctrl_bin, bin;
    ScheduleFlag flag = ScheduleFlag::kFinishBlock;
    ctrl_bin << flag;
    FinishedBlock b{block.id, node_.id, qid_, node_.hostname};
    bin << b;
    Message msg;
    msg.meta.sender = qid_;
    msg.meta.recver = 0;  // TODO The qid of the assigner
    msg.meta.flag = Flag::kOthers;
    msg.AddData(ctrl_bin.ToSArray());
    msg.AddData(bin.ToSArray());
    sender_->Send(std::move(msg));
    VLOG(1) << "Finish block: " << b.DebugString();

    std::unique_lock<std::mutex> lk(mu_);
    num_finished_ += 1;
    cond_.notify_one();
  });
}

}  // namespace xyz

