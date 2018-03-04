#pragma once

#include <string>
#include <vector>

namespace xyz {

class AbstractBlockReader {
public:
  virtual ~AbstractBlockReader() {}

  // call Init before reading.
  virtual void Init(std::string url, size_t offset) = 0;

  // read block api.
  virtual std::vector<std::string> ReadBlock() = 0;

  /*
   * Usage:
   * HdfsBlockReader block_reader(namenode, port);
   * block_reader.Init(url, offset);
   * while (block_reader.HasLine()) {
   *   auto s = block_reader.GetLine();
   * }
   * c += block_reader.GetNumLineRead();
   */
  // Iterator based api.
  virtual bool HasLine() = 0;
  virtual std::string GetLine() = 0;
  virtual int GetNumLineRead() = 0;
};

} // namespace xyz
