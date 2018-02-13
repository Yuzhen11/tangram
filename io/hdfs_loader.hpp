#pragma once

#include <string>
#include "boost/utility/string_ref.hpp"
#include "comm/abstract_sender.hpp"
#include "base/actor.hpp"
#include "base/message.hpp"
#include "base/node.hpp"
#include "base/sarray_binstream.hpp"
#include "base/threadsafe_queue.hpp"
#include "glog/logging.h"
#include "hdfs/hdfs.h"

namespace xyz {

class HdfsLoader: public Actor {
 public:
  HdfsLoader(int qid, std::shared_ptr<AbstractSender> sender, const Node& node)
      : Actor(qid), sender_(sender), app_thread_id_(node.id), hostname_(node.hostname) {
    Start();
  }
  virtual ~HdfsLoader() {
    Stop();
  }
  virtual void Process(Message msg) override;
  void FetchBlock(SArrayBinStream bin);
  void Init(std::string hdfs_namenode, int hdfs_namenode_port, std::string& url);
  void init_blocksize(hdfsFS fs, const std::string& url);
  void Load();
  bool next(boost::string_ref& ref);
  size_t find_next(boost::string_ref sref, size_t l, char c);
  void handle_next_block();
  bool fetch_new_block();
  int read_block(const std::string& fn);
  boost::string_ref fetch_block(bool is_next);

 private:
  std::shared_ptr<AbstractSender> sender_;
  std::string fn_;
  char* data_;
  hdfsFile file_ = NULL;
  hdfsFS fs_;
  size_t hdfs_block_size_;
  size_t offset_ = 0;

  int l = 0;
  int r = 0;
  std::string url_;
  std::string hostname_;
  std::string last_part_;
  boost::string_ref buffer_;

  int app_thread_id_;

  std::vector<std::string> partition_vec_;
};

}  // namespace xyz
