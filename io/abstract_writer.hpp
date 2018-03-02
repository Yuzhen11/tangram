#pragma once

#include <string>

namespace xyz {

struct AbstractWriter {
  virtual ~AbstractWriter() {}
  virtual int Write(std::string dest_url, const void* buffer, size_t len) = 0;
};

}  // namespace xyz

