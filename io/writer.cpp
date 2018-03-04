#include "io/writer.hpp"

namespace xyz {

void Writer::Write(int collection_id, int part_id, std::string dest_url,
                   std::function<void(SArrayBinStream bin)> finish_handle) {
  executor_->Add([this, collection_id, part_id, dest_url, finish_handle]() {
    // 1. write
    SArrayBinStream bin;
    CHECK(partition_manager_->Has(collection_id, part_id));
    partition_manager_->Get(collection_id, part_id)->partition->ToBin(bin);
    auto hdfs_writer = writer_getter_();
    bool rc = hdfs_writer->Write(dest_url, bin.GetPtr(), bin.Size());
    CHECK_EQ(rc, 0);

    // 2. reply
    SArrayBinStream reply_bin;
    reply_bin << qid_;
    finish_handle(reply_bin);
  });
}

} // namespace xyz
