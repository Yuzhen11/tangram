#pragma once

#include <vector>
#include <string>

namespace xyz {

struct BlockInfo {
  std::string filename;
  size_t offset;
  std::string hostname;
};

class AbstractBrowser {
 public:
  virtual ~AbstractBrowser() {}

  virtual std::vector<BlockInfo> Browse(std::string url) = 0;
};

}  // namespace xyz

