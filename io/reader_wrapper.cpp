#include "io/reader_wrapper.hpp"

#include "core/partition/seq_partition.hpp"

#include "core/scheduler/control.hpp"

namespace xyz {

void ReaderWrapper::ReadBlock(AssignedBlock block,
                  std::function<std::shared_ptr<AbstractPartition>(
                    std::shared_ptr<AbstractBlockReader>)> read_func, 
                  std::function<void(SArrayBinStream bin)> finish_handle) {
  num_added_ += 1;
  executor_->Add([this, block, read_func, finish_handle]() {
    // 1. read
    CHECK(block_reader_getter_);
    auto block_reader = block_reader_getter_();
    block_reader->Init(block.url, block.offset);
    auto part = read_func(block_reader);
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

void ReaderWrapper::ReadBlock(AssignedBlock block,
                  std::function<void(SArrayBinStream bin)> finish_handle) {
  ReadBlock(block, [](std::shared_ptr<AbstractBlockReader> block_reader) {
    auto strs = block_reader->ReadBlock();
    auto part = std::make_shared<SeqPartition<std::string>>();
    for (auto &s : strs) {
      part->Add(std::move(s));
    } // TODO: make it more efficient
    return part;
  }, finish_handle);
}

} // namespace xyz
