#include "io/hdfs_reader.hpp"
#include "glog/logging.h"

namespace xyz {

void HdfsReader::Init(std::string namenode, int port, std::string url,
                      size_t offset) {
  InitHdfs(namenode, port, url);

  fn_ = url;
  offset_ = offset;
  size_t offset_ = 0;
  int l = 0;
  int r = 0;

  bool success = fetch_new_block();
  CHECK(success);
}
bool HdfsReader::HasLine() { return next(tmp_line_); }
std::string HdfsReader::GetLine() {
  tmp_line_count_ += 1;
  if (tmp_line_count_ % 10000 == 0) {
    LOG(INFO) << tmp_line_count_ << " lines read.";
  }
  return tmp_line_.to_string();
}
int HdfsReader::GetNumLineRead() { return tmp_line_count_; }

std::vector<std::string> HdfsReader::ReadBlock() {
  std::vector<std::string> ret;
  int c = 0;
  boost::string_ref line;
  while (next(line)) {
    auto s = line.to_string();
    // LOG(INFO) << "Read: " << s;
    ret.push_back(s);
    c++;
    if (c % 10000 == 0) {
      LOG(INFO) << c << " lines read.";
    }
  }
  LOG(INFO) << c << " lines read.";
  return ret;
}

void HdfsReader::InitHdfs(std::string hdfs_namenode, int hdfs_namenode_port,
                          std::string url) {
  struct hdfsBuilder *builder = hdfsNewBuilder();
  hdfsBuilderSetNameNode(builder, hdfs_namenode.c_str());
  hdfsBuilderSetNameNodePort(builder, hdfs_namenode_port);
  fs_ = hdfsBuilderConnect(builder);
  CHECK(fs_);
  hdfsFreeBuilder(builder);
  InitBlocksize(fs_, url);
  CHECK(data_ == nullptr);
  data_ = new char[hdfs_block_size_];
}

void HdfsReader::InitBlocksize(hdfsFS fs, std::string url) {
  int num_files;
  hdfsFileInfo *file_info = hdfsListDirectory(fs, url.c_str(), &num_files);
  for (int i = 0; i < num_files; ++i) {
    if (file_info[i].mKind == kObjectKindFile) {
      hdfs_block_size_ = file_info[i].mBlockSize;
      hdfsFreeFileInfo(file_info, num_files);
      return;
    }
    continue;
  }
  LOG(ERROR) << "Block size init error. (File NOT exist or EMPTY directory)";
}

bool HdfsReader::next(boost::string_ref &ref) {
  if (buffer_.size() == 0) {
    return false;
  }
  // last charater in block
  if (r == buffer_.size() - 1) {
    // fetch next block
    buffer_ = fetch_next();
    if (buffer_.empty()) {
      // end of a file
      // TODO
      return false;
      // bool success = fetch_new_block();
      // if (success == false)
      //   return false;
    } else {
      // directly process the remaing
      last_part_ = "";
      handle_next_block();
      ref = last_part_;
      return true;
    }
  }
  if (offset_ == 0 && r == 0) {
    // begin of a file
    l = 0;
    if (buffer_[0] == '\n')
      // for the case file starting with '\n'
      l = 1;
    r = find_next(buffer_, l, '\n');
  } else {
    // what if not found
    l = find_next(buffer_, r, '\n') + 1;
    r = find_next(buffer_, l, '\n');
  }
  // if the right end does not exist in current block
  if (r == boost::string_ref::npos) {
    auto last = buffer_.substr(l);
    last_part_ = std::string(last.data(), last.size());
    // fetch next subBlock
    buffer_ = fetch_next();
    handle_next_block();
    ref = last_part_;
    return true;
  } else {
    ref = buffer_.substr(l, r - l);
    return true;
  }
}

size_t HdfsReader::find_next(boost::string_ref sref, size_t l, char c) {
  size_t r = l;
  while (r != sref.size() && sref[r] != c)
    r++;
  if (r == sref.size())
    return boost::string_ref::npos;
  return r;
}

void HdfsReader::handle_next_block() {
  while (true) {
    if (buffer_.empty())
      return;

    r = find_next(buffer_, 0, '\n');
    if (r == boost::string_ref::npos) {
      // fetch next subBlock
      last_part_ += std::string(buffer_.data(), buffer_.size());
      buffer_ = fetch_next();
      continue;
    } else {
      last_part_ += std::string(buffer_.substr(0, r).data(), r);
      // TODO
      // clear_buffer();
      buffer_.clear();
      return;
    }
  }
}

bool HdfsReader::fetch_new_block() {
  // fetch a new block
  int nbytes = 0;
  if (fn_.empty()) {
    // no more files
    return "";
  }
  if (file_ != NULL) {
    int rc = hdfsCloseFile(fs_, file_);
    CHECK(rc == 0) << "close file fails";
    // Notice that "file" will be deleted inside hdfsCloseFile
    file_ = NULL;
  }
  // read block
  nbytes = read_block(fn_);
  buffer_ = boost::string_ref(data_, nbytes);

  if (buffer_.empty())
    //  no more files, exit
    return false;
  l = r = 0;
  return true;
}

int HdfsReader::read_block(const std::string &fn) {
  file_ = hdfsOpenFile(fs_, fn.c_str(), O_RDONLY, 0, 0, 0);
  CHECK(file_ != NULL) << "Hadoop file ile open fails";
  hdfsSeek(fs_, file_, offset_);
  size_t start = 0;
  size_t nbytes = 0;
  while (start < hdfs_block_size_) {
    // only 128KB per hdfsRead
    nbytes = hdfsRead(fs_, file_, data_ + start, hdfs_block_size_);
    start += nbytes;
    if (nbytes == 0)
      break;
  }
  return start;
}

boost::string_ref HdfsReader::fetch_next() {
  int nbytes = hdfsRead(fs_, file_, data_, hdfs_block_size_);
  if (nbytes == 0)
    return "";
  if (nbytes == -1) {
    LOG(ERROR) << "read next block error!";
  }
  return boost::string_ref(data_, nbytes);
}

} // namespace xyz
