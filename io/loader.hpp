#pragma once

#include <condition_variable>
#include <mutex>

#include "base/message.hpp"
#include "base/node.hpp"
#include "base/sarray_binstream.hpp"
#include "io/abstract_reader.hpp"
#include "io/meta.hpp"

#include "core/executor/executor.hpp"
#include "core/partition/partition_manager.hpp"

namespace xyz {

class Loader {
public:
  Loader(int qid, std::shared_ptr<Executor> executor,
         std::shared_ptr<PartitionManager> partition_manager,
         std::string namenode, int port, Node node,
         std::function<std::shared_ptr<AbstractReader>()> reader_getter)
      : qid_(qid), executor_(executor), partition_manager_(partition_manager),
        namenode_(namenode), port_(port), node_(node),
        reader_getter_(reader_getter) {}

  ~Loader() {
    std::unique_lock<std::mutex> lk(mu_);
    cond_.wait(lk, [this]() { return num_finished_ == num_added_; });
  }

  // load and store as string
  void Load(AssignedBlock block,
            std::function<void(SArrayBinStream bin)> finish_handle);

  void Load(AssignedBlock block,
            std::function<void(SArrayBinStream bin)> finish_handle,
            std::function<std::shared_ptr<AbstractPartition>(
                std::shared_ptr<AbstractReader>)>
                reader);

private:
  std::shared_ptr<Executor> executor_;
  std::shared_ptr<PartitionManager> partition_manager_;

  std::string namenode_;
  int port_;

  int qid_;
  Node node_;

  int num_added_ = 0;
  int num_finished_ = 0;
  std::mutex mu_;
  std::condition_variable cond_;
  std::function<std::shared_ptr<AbstractReader>()> reader_getter_;
};

} // namespace xyz
