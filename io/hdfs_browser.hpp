#pragma once

#include "io/abstract_browser.hpp"

#include "hdfs/hdfs.h"

namespace xyz {

class HDFSBrowser : public AbstractBrowser {
 public:
  HDFSBrowser(std::string hdfs_namenode, int port);
  virtual ~HDFSBrowser() {}
  virtual std::vector<BlockInfo> Browse(std::string url) override;

  bool InitHDFS(std::string hdfs_namenode, int port);
 private:
  std::string hdfs_namenode_;
  int hdfs_namenode_port_;
  std::string url_;

  hdfsFS fs_ = NULL;
};


}  // namespace xyz

