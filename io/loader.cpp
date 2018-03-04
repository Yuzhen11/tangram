#include "io/loader.hpp"

#include "core/partition/seq_partition.hpp"

#include "core/scheduler/control.hpp"

namespace xyz {

void Loader::Load(AssignedBlock block,
                  std::function<void(SArrayBinStream bin)> finish_handle,
                  std::function<std::shared_ptr<AbstractPartition>(
                      std::shared_ptr<AbstractReader>)>
                      read_func) {
  num_added_ += 1;
  executor_->Add([this, block, read_func, finish_handle]() {
    // 1. read
    CHECK(reader_getter_);
    auto reader = reader_getter_();
    reader->Init(namenode_, port_, block.url, block.offset);
    auto part = read_func(reader);
    partition_manager_->Insert(block.collection_id, block.id, std::move(part));

    // 2. reply
    SArrayBinStream bin;
    FinishedBlock b{block.id, node_.id, qid_, node_.hostname,
                    block.collection_id};
    bin << b;
    finish_handle(bin);
    VLOG(1) << "Finish block: " << b.DebugString();

    std::unique_lock<std::mutex> lk(mu_);
    num_finished_ += 1;
    cond_.notify_one();
  });
}

void Loader::Load(AssignedBlock block,
                  std::function<void(SArrayBinStream bin)> finish_handle) {
  Load(block, finish_handle, [](std::shared_ptr<AbstractReader> reader) {
    auto strs = reader->ReadBlock();
    auto part = std::make_shared<SeqPartition<std::string>>();
    for (auto &s : strs) {
      part->Add(std::move(s));
    } // TODO: make it more efficient
    return part;
  });
}

} // namespace xyz
