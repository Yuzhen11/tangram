#pragma once

#include <condition_variable>
#include <mutex>

#include "base/message.hpp"
#include "base/sarray_binstream.hpp"
#include "io/abstract_reader.hpp"
#include "io/abstract_writer.hpp"
#include "io/meta.hpp"

#include "core/executor/executor.hpp"
#include "core/partition/partition_manager.hpp"
#include "core/plan/function_store.hpp"
#include "core/partition/seq_partition.hpp"
#include "core/scheduler/control.hpp"

namespace xyz {

class IOWrapper {
public:
  IOWrapper(int qid, std::shared_ptr<Executor> executor, std::shared_ptr<PartitionManager> partition_manager, std::shared_ptr<FunctionStore> func_store,
         std::function<std::shared_ptr<AbstractReader>()> reader_getter, std::function<std::shared_ptr<AbstractWriter>()> writer_getter)
      : qid_(qid), executor_(executor), partition_manager_(partition_manager), func_store_(func_store),
        reader_getter_(reader_getter), writer_getter_(writer_getter) {}

  void Read(int collection_id, int part_id, std::string url,
           std::function<void(SArrayBinStream bin)> finish_handle);

  /*
   * write_func : user-defined writer
   * The Write function calls the lambda to generate an SArrayBinStream
   */
  void Write(int collection_id, int part_id, std::string url,
             std::function<void(std::shared_ptr<AbstractPartition>, 
                 std::shared_ptr<AbstractWriter> writer, std::string url)> write_func,
             std::function<void(SArrayBinStream bin)> finish_handle);

private:
  int qid_;
  size_t file_size_ = 0;
  std::shared_ptr<Executor> executor_;
  std::shared_ptr<PartitionManager> partition_manager_;
  std::shared_ptr<FunctionStore> func_store_;
  std::function<std::shared_ptr<AbstractReader>()> reader_getter_;
  std::function<std::shared_ptr<AbstractWriter>()> writer_getter_;
};

} // namespace xyz
