#pragma once

#include <condition_variable>
#include <mutex>

#include "base/message.hpp"
#include "base/node.hpp"
#include "base/sarray_binstream.hpp"
#include "io/abstract_block_reader.hpp"
#include "io/meta.hpp"

#include "core/executor/executor.hpp"
#include "core/partition/partition_manager.hpp"

namespace xyz {

class BlockReaderWrapper {
public:
  BlockReaderWrapper(int qid, std::shared_ptr<Executor> executor,
         std::shared_ptr<PartitionManager> partition_manager,
         Node node,
         std::function<std::shared_ptr<AbstractBlockReader>()> block_reader_getter)
      : qid_(qid), executor_(executor), partition_manager_(partition_manager),
        node_(node),
        block_reader_getter_(block_reader_getter) {}

  ~BlockReaderWrapper() {
    std::unique_lock<std::mutex> lk(mu_);
    cond_.wait(lk, [this]() { return num_finished_ == num_added_; });
  }

  void ReadBlock(AssignedBlock block,
            std::function<std::shared_ptr<AbstractPartition>(
                std::shared_ptr<AbstractBlockReader>)> block_reader,
            std::function<void(SArrayBinStream bin)> finish_handle);

  // deprecated
  void ReadBlock(AssignedBlock block,
            std::function<void(SArrayBinStream bin)> finish_handle);

private:
  std::shared_ptr<Executor> executor_;
  std::shared_ptr<PartitionManager> partition_manager_;

  int qid_;
  Node node_;

  int num_added_ = 0;
  int num_finished_ = 0;
  std::mutex mu_;
  std::condition_variable cond_;
  std::function<std::shared_ptr<AbstractBlockReader>()> block_reader_getter_;
};

} // namespace xyz
