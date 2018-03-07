#include "io/hdfs_writer.hpp"

#include "glog/logging.h"
#include "hdfs/hdfs.h"
#include "io/hdfs_helper.hpp"

namespace xyz {

int HdfsWriter::Write(std::string dest_url, const void *buffer, size_t len) {
  hdfsFS fs = GetFS(hdfs_namenode_, hdfs_namenode_port_);
  std::string dir = dest_url.substr(0, dest_url.find_last_of("/"));
  // LOG(INFO) << "url: " << dest_url;
  // LOG(INFO) << "dir: " << dir;
  int rc = hdfsCreateDirectory(fs, dir.c_str());
  CHECK_EQ(rc, 0) << "cannot create directory: " << dir;
  hdfsFile file = hdfsOpenFile(fs, dest_url.c_str(), O_WRONLY, 0, 0, 0);
  CHECK(hdfsFileIsOpenForWrite(file)) << "cannot open file: " << dest_url;

  if (len > 0) {
    int num_written = hdfsWrite(fs, file, buffer, len);
    CHECK_EQ(num_written, len);
  }
  rc = hdfsFlush(fs, file);
  CHECK_EQ(rc, 0);
  rc = hdfsCloseFile(fs, file);
  CHECK_EQ(rc, 0);
  return 0;
}

} // namespace xyz
