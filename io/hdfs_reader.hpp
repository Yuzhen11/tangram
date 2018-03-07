#pragma once

#include "io/abstract_reader.hpp"
#include "hdfs/hdfs.h"

namespace xyz {

class HdfsReader : public AbstractReader {
public:
  HdfsReader(std::string hdfs_namenode, int hdfs_namenode_port)
      : hdfs_namenode_(hdfs_namenode), hdfs_namenode_port_(hdfs_namenode_port) {
  }
  virtual void Init(std::string url) override;
  void InitFilesize(hdfsFS fs, std::string url);
  virtual size_t GetFileSize() override;
  virtual int Read(void *buffer, size_t len) override;

private:
  std::string hdfs_namenode_;
  int hdfs_namenode_port_;
  std::string url_;
  size_t offset_ = 0;
  size_t hdfs_file_size_;
  hdfsFS fs_;
};

} // namespace xyz