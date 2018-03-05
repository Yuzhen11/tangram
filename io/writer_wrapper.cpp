#include "io/writer_wrapper.hpp"

namespace xyz {

void WriterWrapper::Write(int collection_id, int part_id, std::string dest_url,
                   std::function<void(std::shared_ptr<AbstractPartition>, 
                       std::shared_ptr<AbstractWriter> writer, std::string url)> write_func,
                   std::function<void(SArrayBinStream bin)> finish_handle) {
  executor_->Add([this, collection_id, part_id, dest_url, write_func, finish_handle]() {
    // 1. write
    CHECK(partition_manager_->Has(collection_id, part_id));
    auto part = partition_manager_->Get(collection_id, part_id)->partition;
    // partition_manager_->Get(collection_id, part_id)->partition->ToBin(bin);
    CHECK(writer_getter_);
    auto writer = writer_getter_();
    CHECK(write_func);
    write_func(part, writer, dest_url);

    // 2. reply
    SArrayBinStream reply_bin;
    reply_bin << qid_;
    finish_handle(reply_bin);
  });
}

} // namespace xyz
