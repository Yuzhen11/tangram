#pragma once

#include "io/abstract_reader.hpp"
#include <string>
#include <vector>

namespace xyz {

struct FakeReader : public AbstractReader {
public:
  virtual std::vector<std::string> ReadBlock() override {
    return {"a", "b", "c"};
  }
  virtual void Init(std::string namenode, int port, std::string url,
                    size_t offset) {}
  virtual bool HasLine() {}
  virtual std::string GetLine() {}
  virtual int GetNumLineRead() {}
};

} // namespace xyz
