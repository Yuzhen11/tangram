#pragma once

#include "io/abstract_writer.hpp"

namespace xyz {

class HdfsWriter : public AbstractWriter {
public:
  HdfsWriter(std::string hdfs_namenode, int hdfs_namenode_port)
      : hdfs_namenode_(hdfs_namenode), hdfs_namenode_port_(hdfs_namenode_port) {
  }
  virtual int Write(std::string dest_url, const void *buffer,
                    size_t len) override;

private:
  std::string hdfs_namenode_;
  int hdfs_namenode_port_;
};

} // namespace xyz