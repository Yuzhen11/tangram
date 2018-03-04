#include "io/hdfs_browser.hpp"

#include "glog/logging.h"

namespace xyz {

HDFSBrowser::HDFSBrowser(std::string hdfs_namenode, int port)
    : hdfs_namenode_(hdfs_namenode), hdfs_namenode_port_(port) {
  bool suc = InitHDFS(hdfs_namenode_, hdfs_namenode_port_);
  CHECK(suc) << "Failed to connect to HDFS " << hdfs_namenode_ << ":"
             << hdfs_namenode_port_;
  LOG(INFO) << "Connect to HDFS, namenode:" << hdfs_namenode_
            << " port:" << hdfs_namenode_port_;
}

std::vector<BlockInfo> HDFSBrowser::Browse(std::string url) {
  CHECK(fs_);
  CHECK_EQ(hdfsExists(fs_, url.c_str()), 0) << "url: " << url << "does not exit in hdfs"
          << " <namenode,port>:<" << hdfs_namenode_ << "," << hdfs_namenode_port_ << ">.";
  std::vector<BlockInfo> rets;
  int num_files;
  int dummy;
  hdfsFileInfo *file_info = hdfsListDirectory(fs_, url.c_str(), &num_files);
  for (int i = 0; i < num_files; ++i) {
    // for every file in a directory
    if (file_info[i].mKind != kObjectKindFile)
      continue;
    size_t k = 0;
    while (k < file_info[i].mSize) {
      // for every block in a file
      auto blk_loc =
          hdfsGetFileBlockLocations(fs_, file_info[i].mName, k, 1, &dummy);
      for (int j = 0; j < blk_loc->numOfNodes; ++j) {
        // for every replication in a block
        std::string hostname = blk_loc->hosts[j];
        BlockInfo b{std::string(file_info[i].mName) + '\0', k, hostname};
        rets.push_back(b);
      }
      k += file_info[i].mBlockSize;
    }
  }
  hdfsFreeFileInfo(file_info, num_files);
  return rets;
}

bool HDFSBrowser::InitHDFS(std::string hdfs_namenode, int port) {
  int num_retries = 3;
  while (num_retries--) {
    struct hdfsBuilder *builder = hdfsNewBuilder();
    hdfsBuilderSetNameNode(builder, hdfs_namenode.c_str());
    hdfsBuilderSetNameNodePort(builder, port);
    fs_ = hdfsBuilderConnect(builder);
    hdfsFreeBuilder(builder);
    if (fs_)
      break;
  }
  if (fs_) {
    return true;
  }
  return false;
}

} // namespace xyz
