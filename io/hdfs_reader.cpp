#include "io/hdfs_reader.hpp"

#include "glog/logging.h"
#include "io/hdfs_helper.hpp"

namespace xyz {

void HdfsReader::Init(std::string url) {
  url_ = url;
  fs_ = GetFS(hdfs_namenode_, hdfs_namenode_port_);
  CHECK_EQ(hdfsExists(fs_, url.c_str()), 0);
  HdfsReader::InitFilesize(fs_, url); 
}

void HdfsReader::InitFilesize(hdfsFS fs, std::string url) {
  hdfsFileInfo *file_info = hdfsGetPathInfo(fs, url.c_str());
  CHECK_EQ(file_info[0].mKind, kObjectKindFile);
  hdfs_file_size_ = file_info[0].mSize;
  // LOG(INFO) << "File size: " << std::to_string(hdfs_file_size_);
  hdfsFreeFileInfo(file_info, 1);
}

size_t HdfsReader::GetFileSize() {
  return hdfs_file_size_;
}

int HdfsReader::Read(void *buffer, size_t len) {
  hdfsFile file = hdfsOpenFile(fs_, url_.c_str(), O_RDONLY, 0, 0, 0);
  CHECK(hdfsFileIsOpenForRead(file));
  size_t start = 0;
  size_t nbytes = 0;
  while (start < hdfs_file_size_) {
    // only 128KB per hdfsRead
    nbytes = hdfsRead(fs_, file, buffer + start, hdfs_file_size_);
    start += nbytes;
    if (nbytes == 0)
      break;
  }
  CHECK_EQ(start, hdfs_file_size_);
  int rc = hdfsCloseFile(fs_, file);
  CHECK_EQ(rc, 0);
  return 0;
}

} // namespace xyz
