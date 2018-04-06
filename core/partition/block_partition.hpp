#pragma once

#include <functional>

#include "core/partition/abstract_partition.hpp"
#include "io/meta.hpp"
#include "io/abstract_block_reader.hpp"

namespace xyz {

class BlockPartition : public TypedPartition<std::string> {
 public:
  BlockPartition(AssignedBlock block, 
      std::function<std::shared_ptr<AbstractBlockReader>()> block_reader_getter)
    : block_(block), block_reader_getter_(block_reader_getter) {
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

  std::shared_ptr<AbstractBlockReader> GetReader() {
    CHECK(block_reader_getter_);
    auto block_reader = block_reader_getter_();
    block_reader->Init(block_.url, block_.offset);
    return block_reader;
  }
 private:
  AssignedBlock block_;
  std::function<std::shared_ptr<AbstractBlockReader>()> block_reader_getter_;
};

}  // namespace xyz

