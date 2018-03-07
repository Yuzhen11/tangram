#pragma once

#include <condition_variable>
#include <mutex>

#include "base/message.hpp"
#include "base/sarray_binstream.hpp"
#include "io/abstract_reader.hpp"
#include "io/meta.hpp"

#include "core/executor/executor.hpp"
#include "core/partition/partition_manager.hpp"

namespace xyz {

class ReaderWrapper {
public:
  ReaderWrapper(int qid, std::shared_ptr<Executor> executor,
         std::shared_ptr<PartitionManager> partition_manager,
         std::function<std::shared_ptr<AbstractReader>()> reader_getter)
      : qid_(qid), executor_(executor), partition_manager_(partition_manager),
        reader_getter_(reader_getter) {}

  void Read(int collection_id, int part_id, std::string url,
           std::function<void(SArrayBinStream bin)> finish_handle);

private:
  int qid_;
  size_t file_size_ = 0;
  std::shared_ptr<Executor> executor_;
  std::shared_ptr<PartitionManager> partition_manager_;
  std::function<std::shared_ptr<AbstractReader>()> reader_getter_;
};

} // namespace xyz
