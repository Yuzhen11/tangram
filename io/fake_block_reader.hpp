#pragma once

#include "io/abstract_block_reader.hpp"
#include <string>
#include <vector>

namespace xyz {

struct FakeBlockReader : public AbstractBlockReader {
public:
  virtual std::vector<std::string> ReadBlock() override {
    return {"a", "b", "c"};
  }
  virtual void Init(std::string url, size_t offset) {}
  virtual bool HasLine() {}
  virtual std::string GetLine() {}
  virtual int GetNumLineRead() {}
};

} // namespace xyz
