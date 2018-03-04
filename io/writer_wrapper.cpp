#include "io/writer_wrapper.hpp"

namespace xyz {

void WriterWrapper::Write(int collection_id, int part_id, std::string dest_url,
                   std::function<SArrayBinStream(std::shared_ptr<AbstractPartition>)> part_to_bin,
                   std::function<void(SArrayBinStream bin)> finish_handle) {
  executor_->Add([this, collection_id, part_id, dest_url, part_to_bin, finish_handle]() {
    // 1. write
    SArrayBinStream bin;
    CHECK(partition_manager_->Has(collection_id, part_id));
    auto part = partition_manager_->Get(collection_id, part_id)->partition;
    // partition_manager_->Get(collection_id, part_id)->partition->ToBin(bin);
    CHECK(part_to_bin);
    CHECK(writer_getter_);
    bin = part_to_bin(part);
    auto writer = writer_getter_();
    bool rc = writer->Write(dest_url, bin.GetPtr(), bin.Size());
    CHECK_EQ(rc, 0);

    // 2. reply
    SArrayBinStream reply_bin;
    reply_bin << qid_;
    finish_handle(reply_bin);
  });
}

} // namespace xyz
