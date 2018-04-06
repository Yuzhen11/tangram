#pragma once

#include <functional>

#include "core/partition/abstract_partition.hpp"
#include "io/meta.hpp"
#include "io/abstract_reader.hpp"

namespace xyz {

class FilePartition : public TypedPartition<std::string> {
 public:
  FilePartition(AssignedBlock block, 
      std::function<std::shared_ptr<AbstractReader>()> reader_getter)
    : block_(block), reader_getter_(reader_getter) {
  }
  virtual void TypedAdd(std::string s) override {
    CHECK(false) << "not implemented";
  }

  virtual size_t GetSize() const override { 
    CHECK(false) << "not implemented";
  }

  virtual void FromBin(SArrayBinStream& bin) override {
    CHECK(false) << "not implemented";
  }
  virtual void ToBin(SArrayBinStream& bin) override {
    CHECK(false) << "not implemented";
    // TODO: serialize the reader? or get the function from other place
  }
  virtual typename TypedPartition<std::string>::IterWrapper CreateIterator(bool is_begin) override {
    CHECK(false) << "not implemented";
  }

  std::string GetFileString() {
    CHECK_EQ(block_.offset, 0);  // the block should be a file
    auto reader = reader_getter_();
    reader->Init(block_.url);
    size_t file_size = reader->GetFileSize();
    CHECK_GT(file_size, 0);

    std::string file_str;
    file_str.resize(file_size);
    reader->Read(&file_str[0], file_size);
    return file_str;
  }
 private:
  AssignedBlock block_;
  std::function<std::shared_ptr<AbstractReader>()> reader_getter_;
};

}  // namespace xyz


