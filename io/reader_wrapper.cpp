#include "io/reader_wrapper.hpp"

#include "core/partition/seq_partition.hpp"

#include "core/scheduler/control.hpp"

namespace xyz {

void ReaderWrapper::Read(int collection_id, int part_id, std::string url,
                   std::function<void(SArrayBinStream bin)> finish_handle) {
  executor_->Add([this, collection_id, part_id, url, finish_handle]() {
    // 1. read
    CHECK(reader_getter_);
    auto reader = reader_getter_();
    reader->Init(url);
    file_size_ = reader->GetFileSize();
    CHECK_NE(file_size_, 0);
    LOG(INFO) << "file_size_: " << std::to_string(file_size_);
    char *data = new char[file_size_];
    reader->Read(data, file_size_);
    LOG(INFO) << "data: " << data;

    // 2. put readed data into partition_manager
    std::shared_ptr<AbstractPartition> p;
    SArrayBinStream bin;
    bin.AddBin(data, file_size_);
    delete [] data;
    LOG(INFO) << "bin: " << bin.GetPtr(); 
    p->FromBin(bin);
    // partition_manager_->Insert(collection_id, part_id, std::move(p));

    // 3. reply
    SArrayBinStream reply_bin;
    reply_bin << qid_;
    finish_handle(reply_bin);
  });
}

} // namespace xyz
