#pragma once

#include "io/abstract_reader.hpp"

#include "boost/utility/string_ref.hpp"
#include "hdfs/hdfs.h"

namespace xyz {

class HdfsReader : public AbstractReader {
public:
  HdfsReader(std::string namenode, int port)
      : namenode_(namenode), port_(port) {}
  ~HdfsReader() { delete[] data_; }
  virtual std::vector<std::string> ReadBlock() override;

  virtual void Init(std::string url, size_t offset) override;
  virtual bool HasLine() override;
  virtual std::string GetLine() override;
  virtual int GetNumLineRead() override;

private:
  void InitHdfs(std::string hdfs_namenode, int hdfs_namenode_port,
                std::string url);

  void InitBlocksize(hdfsFS fs, std::string url);

  bool next(boost::string_ref &ref);
  size_t find_next(boost::string_ref sref, size_t l, char c);
  void handle_next_block();
  bool fetch_new_block();
  int read_block(const std::string &fn);

  boost::string_ref fetch_next();

private:
  char *data_ = nullptr;
  hdfsFS fs_;
  size_t hdfs_block_size_;

  size_t offset_ = 0;
  int l = 0;
  int r = 0;
  std::string last_part_;
  boost::string_ref buffer_;
  std::string fn_;
  hdfsFile file_ = NULL;

  boost::string_ref tmp_line_;
  int tmp_line_count_ = 0;

  std::string namenode_;
  int port_;
};

} // namespace xyz
