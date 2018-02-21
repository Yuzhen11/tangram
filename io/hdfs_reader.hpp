#pragma once

#include "io/abstract_reader.hpp"

#include "boost/utility/string_ref.hpp"
#include "hdfs/hdfs.h"

namespace xyz {

class HdfsReader : public AbstractReader {
 public:
  virtual std::vector<std::string> Read(std::string namenode, int port, 
          std::string url, size_t offset) override;

  void Init(std::string hdfs_namenode, int hdfs_namenode_port, std::string url);

  void InitBlocksize(hdfsFS fs, std::string url);

  bool next(boost::string_ref& ref);
  size_t find_next(boost::string_ref sref, size_t l, char c);
  void handle_next_block();
  bool fetch_new_block();
  int read_block(const std::string& fn);

  boost::string_ref fetch_next();
 private:
  char* data_;
  hdfsFS fs_;
  size_t hdfs_block_size_;

  size_t offset_ = 0;
  int l = 0;
  int r = 0;
  std::string last_part_;
  boost::string_ref buffer_;
  std::string fn_;
  hdfsFile file_ = NULL;
};

}  // namespace xyz
