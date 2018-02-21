#pragma once

#include <vector>
#include <string>

namespace xyz {

class AbstractReader {
 public:
  virtual ~AbstractReader() {}
  virtual std::vector<std::string> Read(std::string namenode, int port, 
          std::string url, size_t offset) = 0;
};

}  // namespace xyz

