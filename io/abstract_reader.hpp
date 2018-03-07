#pragma once

#include <string>

namespace xyz {

struct AbstractReader {
  virtual ~AbstractReader() {}
  virtual void Init(std::string url) = 0;
  virtual size_t GetFileSize() = 0;
  virtual int Read(void *buffer, size_t len) = 0;
};

} // namespace xyz
