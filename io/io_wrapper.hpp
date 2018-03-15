#pragma once

#include <memory>

#include "io/abstract_reader.hpp"
#include "io/abstract_writer.hpp"

namespace xyz {

class IOWrapper {
public:
  IOWrapper(std::function<std::shared_ptr<AbstractReader>()> reader_getter,
            std::function<std::shared_ptr<AbstractWriter>()> writer_getter)
      : reader_getter_(reader_getter), writer_getter_(writer_getter) {}

  std::shared_ptr<AbstractReader> GetReader() {
    CHECK(reader_getter_);
    return reader_getter_();
  }
  std::shared_ptr<AbstractWriter> GetWriter() {
    CHECK(writer_getter_);
    return writer_getter_();
  }
private:
  std::function<std::shared_ptr<AbstractReader>()> reader_getter_;
  std::function<std::shared_ptr<AbstractWriter>()> writer_getter_;
};

} // namespace xyz
