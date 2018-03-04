#pragma once

#include <sstream>
#include <string>
#include <vector>

namespace xyz {

struct BlockInfo {
  std::string filename;
  size_t offset;
  std::string hostname;

  std::string DebugString() const {
    std::stringstream ss;
    ss << "filename: " << filename;
    ss << ", offset: " << offset;
    ss << ", hostname : " << hostname;
    return ss.str();
  }
};

class AbstractBrowser {
public:
  virtual ~AbstractBrowser() {}

  virtual std::vector<BlockInfo> Browse(std::string url) = 0;
};

} // namespace xyz
