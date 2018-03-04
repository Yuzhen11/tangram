#pragma once

#include <string>
#include <vector>

namespace xyz {

class AbstractReader {
public:
  virtual ~AbstractReader() {}

  // call Init before reading.
  virtual void Init(std::string url, size_t offset) = 0;

  // read block api.
  virtual std::vector<std::string> ReadBlock() = 0;

  /*
   * Usage:
   * HdfsReader reader(namenode, port);
   * reader.Init(url, offset);
   * while (reader.HasLine()) {
   *   auto s = reader.GetLine();
   * }
   * c += reader.GetNumLineRead();
   */
  // Iterator based api.
  virtual bool HasLine() = 0;
  virtual std::string GetLine() = 0;
  virtual int GetNumLineRead() = 0;
};

} // namespace xyz
