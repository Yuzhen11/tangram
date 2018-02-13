#pragma once

#include "base/node.hpp"
#include "base/actor.hpp"
#include "base/message.hpp"
#include "base/sarray_binstream.hpp"
#include "base/threadsafe_queue.hpp"
#include "comm/abstract_sender.hpp"
#include <map>
#include <memory>
#include <set>
#include <vector>
#include <string>
#include <unordered_set>
#include <utility>
#include "hdfs/hdfs.h"
#include "zmq.hpp"

namespace xyz {

class HDFSBlockAssigner: public Actor{
 public:
  // 301 is a constant for IO load
  // 300 is for exit procedure
  static const int kBlockFinish = 301;

  struct BlkDesc {
    std::string filename;
    size_t offset;
    std::string block_location;
    bool operator==(const BlkDesc& other) const;
  };

  HDFSBlockAssigner(int qid, std::string hdfsNameNode, int hdfsNameNodePort, AbstractSender* sender);
  ~HDFSBlockAssigner() = default;


 private:
  virtual void Process(Message msg);
  void Init(Node node, const std::vector<Node>& nodes, std::string url, int num_block_slots); 
  void assign_block(int node_id, std::string hostname);
  void init_socket(int master_port, zmq::context_t* zmq_context);
  void init_hdfs(const std::string& node, const int& port);
  void browse_hdfs(const std::string& url);
  std::pair<std::string, size_t> answer(const std::string& host);

 private:
  std::string url_;
  std::string hdfs_namenode_;
  int hdfs_namenode_port_;
  int app_thread_id_;
  int num_block_slots_;
  ThreadsafeQueue<SArrayBinStream>* worker_queue_;
  std::unique_ptr<AbstractSender> sender_;
  hdfsFS fs_ = NULL;
  std::set<int> finished_workers_;
  int num_workers_alive_;

  //{host:[{filename,offset,block_location}, {filename,offset,block_location}...}
  std::pair<std::map<std::string, int>, size_t> finish_dict_;
  // finish_multi_dict_ describes each available thread has request the url, and response null
  // if none thread can get anything from an url, this means this url has dispensed all block, this url can be removed
  // {{host: count},count_num},...}
  std::map<std::string, std::unordered_set<BlkDesc>> files_locality_dict_;
};

}  // namespace flexps

namespace std {
template <>
struct hash<xyz::HDFSBlockAssigner::BlkDesc> {
  size_t operator()(const xyz::HDFSBlockAssigner::BlkDesc& t) const { return hash<string>()(t.filename); }
};
}  // namespace std
