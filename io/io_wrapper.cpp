#include "io/io_wrapper.hpp"

namespace xyz {

struct ObjT {
  using KeyT = int;
  using ValT = int;
  int key;
  int val;
  KeyT Key() const { return key; }
};

void IOWrapper::Read(int collection_id, int part_id, std::string url,
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
    std::string str(data);
    LOG(INFO) << "data: " << str;

    // 2. put readed data into partition_manager
    // auto p = std::make_shared<SeqPartition<ObjT>>();
    auto get_func = func_store_->GetCreatePart(collection_id);
    auto p = get_func();
    SArrayBinStream bin;
    bin.AddBin(data, file_size_);
    delete [] data;
    p->FromBin(bin);
    partition_manager_->Insert(collection_id, part_id, std::move(p));

    // 3. reply
    SArrayBinStream reply_bin;
    reply_bin << qid_;
    finish_handle(reply_bin);
  });
}

void IOWrapper::Write(int collection_id, int part_id, std::string url,
                   std::function<void(std::shared_ptr<AbstractPartition>, 
                       std::shared_ptr<AbstractWriter> writer, std::string url)> write_func,
                   std::function<void(SArrayBinStream bin)> finish_handle) {
  executor_->Add([this, collection_id, part_id, url, write_func, finish_handle]() {
    // 1. write
    CHECK(partition_manager_->Has(collection_id, part_id));
    auto part = partition_manager_->Get(collection_id, part_id)->partition;
    CHECK(writer_getter_);
    auto writer = writer_getter_();
    CHECK(write_func);
    write_func(part, writer, url);

    // 2. reply
    SArrayBinStream reply_bin;
    reply_bin << qid_ << collection_id;
    finish_handle(reply_bin);
  });
}

} // namespace xyz
