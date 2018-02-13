#include "io/hdfs_assigner.hpp"
#include <utility>
#include "base/sarray_binstream.hpp"
#include "glog/logging.h"

namespace xyz {

const int HDFSBlockAssigner::kBlockFinish;

bool HDFSBlockAssigner::BlkDesc::operator==(const BlkDesc& other) const {
  return filename == other.filename && offset == other.offset && block_location == block_location;
}

HDFSBlockAssigner::HDFSBlockAssigner(int qid, std::string hdfsNameNode, int hdfsNameNodePort, AbstractSender* sender) : Actor(qid), sender_(sender), hdfs_namenode_(hdfsNameNode), hdfs_namenode_port_(hdfsNameNodePort){
  init_hdfs(hdfs_namenode_, hdfs_namenode_port_);
  sender_ = std::unique_ptr<AbstractSender>(sender);
}


void HDFSBlockAssigner::Init(Node node, const std::vector<Node>& nodes, std::string url, int num_block_slots) {
  num_block_slots_ = num_block_slots;
  url_ = url;
  app_thread_id_ = node.id;
  num_workers_alive_ = nodes.size();
  browse_hdfs(url);
  for(auto worker_node: nodes) { 
    for(int i = 0; i < num_block_slots; i++)
      assign_block(worker_node.id, worker_node.hostname);
  }
  Start();
}

void HDFSBlockAssigner::Process(Message msg) {
  SArrayBinStream bin1;
  bin1.FromSArray(msg.data[0]);
  int type;
  bin1 >> type;
  if (type == kBlockFinish) {
    std::string hostname;
    SArrayBinStream bin2;
    bin2.FromSArray(msg.data[1]);
    bin2 >> hostname;
    assign_block(msg.meta.sender, hostname);
  } else {
    CHECK(false) << "Unknown message: " << type;
  }
}

void HDFSBlockAssigner::init_hdfs(const std::string& node, const int& port) {
  int num_retries = 3;
  while (num_retries--) {
    struct hdfsBuilder* builder = hdfsNewBuilder();
    hdfsBuilderSetNameNode(builder, node.c_str());
    hdfsBuilderSetNameNodePort(builder, port);
    fs_ = hdfsBuilderConnect(builder);
    hdfsFreeBuilder(builder);
    if (fs_)
      break;
  }
  if (fs_) {
    return;
  }
  LOG(INFO) << "Failed to connect to HDFS " << node << ":" << port;
}

void HDFSBlockAssigner::browse_hdfs(const std::string& url) {
  if (!fs_)
    return;

  int num_files;
  int dummy;
  hdfsFileInfo* file_info = hdfsListDirectory(fs_, url.c_str(), &num_files);
  for (int i = 0; i < num_files; ++i) {
    // for every file in a directory
    if (file_info[i].mKind != kObjectKindFile)
      continue;
    size_t k = 0;
    while (k < file_info[i].mSize) {
      // for every block in a file
      auto blk_loc = hdfsGetFileBlockLocations(fs_, file_info[i].mName, k, 1, &dummy);
      for (int j = 0; j < blk_loc->numOfNodes; ++j) {
        // for every replication in a block
        files_locality_dict_[blk_loc->hosts[j]].insert(
            BlkDesc{std::string(file_info[i].mName) + '\0', k, std::string(blk_loc->hosts[j])});
        // init finish_dict_, 0 means none thread has been rejected by this (url, host)
        (finish_dict_.first)[blk_loc->hosts[j]] = 0;
      }
      k += file_info[i].mBlockSize;
    }
  }
  hdfsFreeFileInfo(file_info, num_files);
}

void HDFSBlockAssigner::assign_block(int node_id, std::string hostname){
  for (int i = 0; i < num_block_slots_; i++) {
    std::pair<std::string, size_t> ret = answer(hostname);
    Message msg;
    msg.meta.sender = app_thread_id_;
    msg.meta.recver = node_id;
    SArrayBinStream bin;
    bin << ret.first << ret.second << url_ << hdfs_namenode_port_ << hdfs_namenode_;
    msg.AddData(bin.ToSArray());
    sender_->Send(std::move(msg));
  }
}

std::pair<std::string, size_t> HDFSBlockAssigner::answer(const std::string& host) {
  if (!fs_)
    return {"", 0};


  // selected_file, offset
  std::pair<std::string, size_t> ret = {"", 0};

  /*
   * selected_host has two situation:
   * 1. when load_locally, selected_host always equals host, or null
   * 2. when load_globally,
   *     when loading local host, selected_host equals host,
   *     when loading gloabl host, selected_host equals an unfinished host
   */
  std::string selected_host;
  //      if (load_type.empty() || load_type == "load_hdfs_globally") { // default is load data globally
  // selected_file
  // if there is local file, allocate local file
  if (files_locality_dict_[host].size()) {
    selected_host = host;
  } else {  // there is no local file, so need to check all hosts
            // when loading data globally, util that all hosts finished means finishing
    bool is_finish = true;
    for (auto& item : files_locality_dict_) {
      // when loading globally, any host havingn't been finished means unfinished
      if (item.second.size() != 0) {
        is_finish = false;
        // in fact, there can be optimizing. for example, everytime, we can calculate the host
        // which has the longest blocks. It may be good for load balance but search is time-consuming
        selected_host = item.first;  // default allocate a unfinished host block
        break;
      }
    }

    if (is_finish) {
      finish_dict_.second += 1;
      if (finish_dict_.second == num_workers_alive_) {
        // this means all workers's requests about this url are rejected
        // blocks under this url are all allocated
        files_locality_dict_.clear();
      }
      return {"", 0};
    }
  }

  // according selected_host to get the select file
  auto selected_file = files_locality_dict_[selected_host].begin();
  // select
  ret = {selected_file->filename, selected_file->offset};

  // cautious: need to remove all replicas in different host
  for (auto its = files_locality_dict_.begin(); its != files_locality_dict_.end(); its++) {
    for (auto it = its->second.begin(); it != its->second.end(); it++) {
      if (it->filename == ret.first && it->offset == ret.second) {
        files_locality_dict_[its->first].erase(it);
        break;
      }
    }
  }

  return ret;
}
}

